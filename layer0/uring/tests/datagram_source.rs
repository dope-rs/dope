use std::{
    net,
    os::fd::{AsFd as _, AsRawFd as _},
    time,
};

use dope_sys::{
    Errno,
    io::socket::{self, Socket as _, addr, msg, msg::source::Source as _},
};
use dope_uring::Uring;

#[test]
fn retains_source_and_port() {
    for (bind, peer) in [
        (
            net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED),
            net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 2)),
            net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V6(net::Ipv6Addr::UNSPECIFIED),
            net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
            net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V6(net::Ipv6Addr::UNSPECIFIED),
            net::IpAddr::V6(net::Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ),
        (
            net::IpAddr::V6(net::Ipv4Addr::UNSPECIFIED.to_ipv6_mapped()),
            net::IpAddr::V6(net::Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ),
    ] {
        let receiver = net::UdpSocket::bind((peer, 0)).expect("receive socket");
        receiver
            .set_read_timeout(Some(time::Duration::from_secs(2)))
            .expect("receive deadline");
        let sender = net::UdpSocket::bind((bind, 0)).expect("send socket");
        let endpoint = sender.local_addr().expect("endpoint address");
        let peer = receiver.local_addr().expect("peer address");
        let (selected, source) = Uring::source(endpoint, peer).expect("select source");
        let mut control = Uring::compose(source, None);
        let target = addr::Internet::from_std::<Uring>(peer);
        let input = [msg::Iovec::from_slice(b"source")];
        let mut header = msg::Header::new();
        header.name(target.as_addr());
        header.bind(&input);
        header.control(Uring::bytes(&mut control));
        // SAFETY: input, target and immutable aligned control remain
        // owned at stable addresses until this synchronous send completes.
        unsafe { <Uring as socket::raw::Source>::sendmsg::<Errno>(sender.as_fd(), &header, 0) }
            .expect("send with selected source");
        let mut packet = [0; 6];
        let (length, actual) = receiver.recv_from(&mut packet).expect("receive packet");
        assert_eq!(&packet[..length], b"source");
        assert_eq!(actual, selected.get());
        assert_eq!(actual.port(), endpoint.port());
        let (restored, gso) = Uring::release(control);
        assert!(gso.is_none());
        assert_eq!(restored.as_ref(), source.as_ref());
    }
}

#[test]
fn datagram_plain() {
    let socket = Uring::datagram::<Errno>(socket::Domain::Inet4).expect("plain datagram socket");
    // SAFETY: both commands read scalar flags from this live descriptor.
    let (flags, status) = unsafe {
        (
            libc::fcntl(socket.as_raw_fd(), libc::F_GETFD),
            libc::fcntl(socket.as_raw_fd(), libc::F_GETFL),
        )
    };
    assert!(flags >= 0 && status >= 0);
    assert_eq!(flags & libc::FD_CLOEXEC, 0);
    assert_eq!(status & libc::O_NONBLOCK, 0);

    let peer =
        net::UdpSocket::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("local datagram receiver");
    peer.set_read_timeout(Some(time::Duration::from_secs(5)))
        .expect("receive deadline");
    let address = addr::Internet::from_std::<Uring>(peer.local_addr().expect("local address"));
    address
        .as_addr()
        .try_connect::<Uring, Errno>(socket.as_fd())
        .expect("connect datagram socket");
    let message = b"datagram";
    assert_eq!(
        Uring::send_now::<Errno>(socket.as_fd(), message).expect("send datagram"),
        message,
    );
    let mut received = [0; 8];
    assert_eq!(
        peer.recv(&mut received).expect("receive datagram"),
        message.len(),
    );
    assert_eq!(&received, message);
}
