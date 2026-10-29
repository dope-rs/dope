use std::{net, num, os::fd, time};

use dope_sys::{
    Errno,
    io::socket::{self, addr, gso, msg, msg::source::Source as _},
};
use dope_uring::Uring;
use gso::Mode as _;

type Control = <Uring as gso::Source>::Mode;

#[test]
fn segments_datagrams() {
    let receiver = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("receive socket");
    receiver
        .set_read_timeout(Some(time::Duration::from_secs(2)))
        .expect("receive deadline");
    let sender = net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("send socket");
    sender
        .connect(receiver.local_addr().expect("receive address"))
        .expect("connect datagrams");
    let mut cap = Control::acquire().expect("native segmentation");
    let mut payload = [0; 2345];
    for (offset, byte) in payload.iter_mut().enumerate() {
        *byte = offset as u8;
    }
    for size in [1000, 600] {
        let size = num::NonZero::new(size).expect("nonzero segment size");
        let mut control = Control::control(cap, size);
        let bytes = control.bytes();
        let iov = [msg::Iovec::from_slice(&payload)];
        let mut header = msg::Header::new();
        header.bind(&iov);
        header.control(bytes);
        // SAFETY: the header borrows the complete payload and aligned control
        // for this synchronous call; both remain owned until it returns.
        let sent = unsafe {
            <Uring as socket::raw::Source>::sendmsg::<Errno>(fd::AsFd::as_fd(&sender), &header, 0)
        }
        .expect("segmented sendmsg");
        assert_eq!(sent.get() as usize, payload.len());
        for expected in payload.chunks(usize::from(size.get())) {
            let mut packet = [0; 2346];
            let received = receiver.recv(&mut packet).expect("segmented datagram");
            assert_eq!(&packet[..received], expected);
        }
        let (released, actual) = control.release();
        assert_eq!(actual, size);
        cap = released;
    }
}

#[test]
fn segments_from_source() {
    for (bind, local, peer) in [
        (
            net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED),
            net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 2)),
            net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V6(net::Ipv6Addr::UNSPECIFIED),
            net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
            net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
        ),
    ] {
        let receiver = net::UdpSocket::bind((peer, 0)).expect("receive socket");
        receiver
            .set_read_timeout(Some(time::Duration::from_secs(2)))
            .expect("receive deadline");
        let sender = net::UdpSocket::bind((bind, 0)).expect("send socket");
        let peer = receiver.local_addr().expect("receive address");
        let local = net::SocketAddr::new(local, sender.local_addr().expect("send address").port());
        let (selected, mut source) = Uring::source(local, peer).expect("select source");
        let address = addr::Internet::from_std::<Uring>(peer);
        let mut cap = Control::acquire().expect("native segmentation");
        let payload = [0x71; 2401];
        for size in [1200, 900] {
            let size = num::NonZero::new(size).expect("nonzero segment size");
            let mut control = Uring::compose(source, Some(Control::control(cap, size)));
            let iov = [msg::Iovec::from_slice(&payload)];
            let mut header = msg::Header::new();
            header.name(address.as_addr());
            header.bind(&iov);
            header.control(Uring::bytes(&mut control));
            // SAFETY: the destination, payload and complete aligned ancillary
            // owner remain alive and unchanged until this synchronous send returns.
            unsafe {
                <Uring as socket::raw::Source>::sendmsg::<Errno>(
                    fd::AsFd::as_fd(&sender),
                    &header,
                    0,
                )
            }
            .expect("sourced segmented sendmsg");
            for expected in payload.chunks(usize::from(size.get())) {
                let mut packet = [0; 2402];
                let (received, actual) =
                    receiver.recv_from(&mut packet).expect("segmented datagram");
                assert_eq!((&packet[..received], actual), (expected, selected.get()));
            }
            let (restored, gso) = Uring::release(control);
            let (released, actual) = gso.expect("retained segmentation").release();
            assert_eq!(actual, size);
            assert_eq!(restored.as_ref(), source.as_ref());
            source = restored;
            cap = released;
        }
    }
}
