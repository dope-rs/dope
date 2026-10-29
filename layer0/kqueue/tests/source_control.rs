use std::{
    mem, net,
    os::fd::{AsFd as _, AsRawFd as _},
    slice, time,
};

use dope_kqueue::Kqueue;
use dope_sys::{
    Errno,
    io::socket::{self, Domain, Socket as _, addr, addr::Internet, msg, msg::source::Source as _},
};

#[test]
fn sends_selected_source() {
    for (bind, ip) in [
        (
            net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED),
            net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
        ),
        (
            net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
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
        let receiver = net::UdpSocket::bind(net::SocketAddr::new(ip, 0)).expect("receiver");
        receiver
            .set_read_timeout(Some(time::Duration::from_secs(1)))
            .expect("timeout");
        let bind = net::SocketAddr::new(bind, 0);
        let sender = net::UdpSocket::bind(bind).expect("sender");
        let peer = receiver.local_addr().expect("peer");
        let (selected, source) =
            Kqueue::source(sender.local_addr().expect("bind"), peer).expect("source selection");
        let mut control = Kqueue::compose(source, None);
        let address = addr::Internet::from_std::<Kqueue>(peer);
        let bytes = [1u8];
        let input = [msg::Iovec::from_slice(&bytes)];
        let mut message = msg::Header::new();
        message.name(address.as_addr());
        message.bind(&input);
        message.control(Kqueue::bytes(&mut control));
        // SAFETY: the input, address and aligned control remain alive at their
        // initialized addresses until this synchronous send completes.
        unsafe { <Kqueue as socket::raw::Source>::sendmsg::<Errno>(sender.as_fd(), &message, 0) }
            .expect("send selected source");
        let mut output = [0u8];
        let (len, actual) = receiver.recv_from(&mut output).expect("datagram");
        assert_eq!((len, output, actual), (1, bytes, selected.get()));
        let (restored, gso) = Kqueue::release(control);
        assert!(gso.is_none());
        assert_eq!(restored.as_ref(), source.as_ref());
    }
}

#[test]
fn local_uses_capacity() {
    let socket = net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("listener");
    let expected = socket.local_addr().expect("bound address");
    let mut storage = addr::Storage::empty();
    *storage.output_mut().1 = 0;
    let view =
        addr::Addr::local::<Kqueue, Errno>(socket.as_fd(), &mut storage).expect("local address");
    assert_eq!(addr::Storage::from_addr(view).to_std(), Ok(expected));
}

#[test]
fn unix_path_extent() {
    let offset = mem::offset_of!(libc::sockaddr_un, sun_path);
    let capacity = mem::size_of::<libc::sockaddr_un>() - offset;
    let maximum = capacity - usize::from(<Kqueue as addr::Source>::UNIX_TERMINATED);
    let bytes = [b'p'; mem::size_of::<libc::sockaddr_un>()];
    let address = addr::Unix::from_path::<Kqueue>(&bytes[..maximum]).expect("native path limit");
    let (raw, length) = address.as_addr().parts();
    assert_eq!(
        length.get(),
        offset + maximum + usize::from(maximum < capacity),
    );
    // SAFETY: the borrowed address retains its full initialized native payload.
    let encoded = unsafe { slice::from_raw_parts(raw.cast::<u8>().add(offset), maximum) };
    assert_eq!(encoded, &bytes[..maximum]);
    assert!(matches!(
        addr::Unix::from_path::<Kqueue>(&bytes[..maximum + 1]),
        Err(addr::PathError::Length { actual, maximum: limit })
            if actual == maximum + 1 && limit == maximum
    ));
}

#[test]
fn unix_abstract_unsupported() {
    assert!(matches!(
        addr::Unix::from_name::<Kqueue>(b"name\0tail"),
        Err(addr::PathError::Unsupported)
    ));
}

#[test]
fn datagram_plain() {
    let socket = Kqueue::datagram::<Errno>(Domain::Inet4).expect("plain datagram socket");
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
    let address = Internet::from_std::<Kqueue>(peer.local_addr().expect("local address"));
    address
        .as_addr()
        .try_connect::<Kqueue, Errno>(socket.as_fd())
        .expect("connect datagram socket");
    let message = b"datagram";
    assert_eq!(
        Kqueue::send_now::<Errno>(socket.as_fd(), message).expect("send datagram"),
        message,
    );
    let mut received = [0; 8];
    assert_eq!(
        peer.recv(&mut received).expect("receive datagram"),
        message.len(),
    );
    assert_eq!(&received, message);
}
