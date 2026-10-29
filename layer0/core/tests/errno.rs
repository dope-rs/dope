use std::{io, net, os::fd};

use dope_core::io::socket;
use fd::AsFd as _;

#[test]
fn bind_retains_errno() {
    let listener = net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("listener");
    let binding = socket::Listener::bind(listener.local_addr().expect("address"));
    assert_eq!(unsafe { libc::close(-1) }, -1);
    assert!(matches!(
        binding,
        Err(socket::BindError::Bind(errno)) if errno.code().get() == libc::EADDRINUSE
    ));
}

#[test]
fn connect_retains_errno() {
    let pipe = io::pipe().expect("pipe");
    for ip in [
        net::IpAddr::V4(net::Ipv4Addr::LOCALHOST),
        net::IpAddr::V6(net::Ipv6Addr::LOCALHOST),
    ] {
        let address = socket::Inet::from_std(net::SocketAddr::new(ip, 0));
        let result = address.try_connect(pipe.0.as_fd());
        assert_eq!(unsafe { libc::close(-1) }, -1);
        assert!(matches!(result, Err(error) if error.code().get() == libc::ENOTSOCK));
    }
}

#[test]
fn connects_peer() {
    use fd::{AsRawFd as _, FromRawFd as _};

    let listener = net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, 0)).expect("listener");
    let address = socket::Inet::from_std(listener.local_addr().expect("address"));
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    assert!(raw >= 0, "socket creation");
    let socket = unsafe { fd::OwnedFd::from_raw_fd(raw) };
    assert_eq!(
        unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) },
        0
    );
    address.try_connect(socket.as_fd()).expect("connect");
    let (peer, _) = listener.accept().expect("accepted peer");
    assert_eq!(
        unsafe { libc::send(socket.as_raw_fd(), b"a".as_ptr().cast(), 1, 0) },
        1
    );
    let mut byte = [0];
    assert_eq!(
        unsafe { libc::recv(peer.as_raw_fd(), byte.as_mut_ptr().cast(), 1, 0) },
        1
    );
    assert_eq!(byte, *b"a");
}
