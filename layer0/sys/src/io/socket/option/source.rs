use std::os::fd;

use crate::{
    Errno,
    io::{
        socket,
        socket::option::{Kind, Value},
    },
};

/// Native ABI selection for the same validated scalar options.
pub trait Source: socket::Socket {
    const KEEP_IDLE: libc::c_int;

    fn option(kind: Kind) -> (libc::c_int, libc::c_int) {
        match kind {
            Kind::NoDelay => (libc::IPPROTO_TCP, libc::TCP_NODELAY),
            Kind::KeepAlive => (libc::SOL_SOCKET, libc::SO_KEEPALIVE),
            Kind::RecvBuffer => (libc::SOL_SOCKET, libc::SO_RCVBUF),
            Kind::SendBuffer => (libc::SOL_SOCKET, libc::SO_SNDBUF),
            Kind::KeepAliveIdle => (libc::IPPROTO_TCP, Self::KEEP_IDLE),
            Kind::KeepAliveInterval => (libc::IPPROTO_TCP, libc::TCP_KEEPINTVL),
            Kind::KeepAliveRetries => (libc::IPPROTO_TCP, libc::TCP_KEEPCNT),
        }
    }

    fn set(fd: fd::BorrowedFd<'_>, kind: Kind, value: &Value) -> Result<(), Errno> {
        let (level, name) = Self::option(kind);
        Self::set_option(fd, level, name, &value.raw())
    }
}
