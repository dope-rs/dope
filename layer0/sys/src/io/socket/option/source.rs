use std::os::fd;

use crate::{
    Errno,
    io::{
        socket,
        socket::option::{Kind, Level, Name, Value},
    },
};

/// Native ABI selection for the same validated scalar options.
pub trait Source: socket::Socket {
    const KEEP_IDLE: Name;

    fn option(kind: Kind) -> (Level, Name) {
        match kind {
            Kind::NoDelay => (Level::new(libc::IPPROTO_TCP), Name::new(libc::TCP_NODELAY)),
            Kind::KeepAlive => (Level::new(libc::SOL_SOCKET), Name::new(libc::SO_KEEPALIVE)),
            Kind::RecvBuffer => (Level::new(libc::SOL_SOCKET), Name::new(libc::SO_RCVBUF)),
            Kind::SendBuffer => (Level::new(libc::SOL_SOCKET), Name::new(libc::SO_SNDBUF)),
            Kind::KeepAliveIdle => (Level::new(libc::IPPROTO_TCP), Self::KEEP_IDLE),
            Kind::KeepAliveInterval => (
                Level::new(libc::IPPROTO_TCP),
                Name::new(libc::TCP_KEEPINTVL),
            ),
            Kind::KeepAliveRetries => (Level::new(libc::IPPROTO_TCP), Name::new(libc::TCP_KEEPCNT)),
        }
    }

    fn set(fd: fd::BorrowedFd<'_>, kind: Kind, value: &Value) -> Result<(), Errno> {
        let (level, name) = Self::option(kind);
        Self::set_option(fd, level.raw(), name.raw(), &value.raw())
    }
}
