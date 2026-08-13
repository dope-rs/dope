mod addr;

pub mod establishment;
pub mod msg;
pub mod option;
pub mod raw;

use std::{io, net};

pub use addr::Addr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct StreamSpec(Domain);

impl StreamSpec {
    pub fn for_peer(peer: &Addr) -> io::Result<Self> {
        Ok(Self(Domain::from_raw(peer.raw().family())?))
    }

    pub(crate) const fn domain(self) -> Domain {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ListenerConfig {
    pub reuse_addr: bool,
    pub reuse_port: bool,
    pub fast_open: FastOpen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FastOpenBacklog(i32);

impl FastOpenBacklog {
    pub const fn new(backlog: i32) -> Option<Self> {
        if backlog > 0 {
            Some(Self(backlog))
        } else {
            None
        }
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Server-side TCP Fast Open policy.
/// [`Required`](Self::Required) configures the listener before binding and
/// fails creation when the platform lacks Fast Open support.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FastOpen {
    #[default]
    Disabled,
    Required {
        backlog: FastOpenBacklog,
    },
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            reuse_addr: true,
            reuse_port: false,
            fast_open: FastOpen::Disabled,
        }
    }
}

impl ListenerConfig {
    pub fn for_datagram(addr: &net::SocketAddr) -> Self {
        let reuse = addr.port() != 0;
        Self {
            reuse_addr: reuse,
            reuse_port: reuse,
            ..Self::default()
        }
    }

    #[doc(hidden)]
    pub fn for_tcp(reuse_port: bool, fast_open: FastOpen) -> Self {
        Self {
            reuse_addr: true,
            reuse_port,
            fast_open,
        }
    }

    pub(crate) const fn fast_open_backlog(&self) -> Option<FastOpenBacklog> {
        match self.fast_open {
            FastOpen::Disabled => None,
            FastOpen::Required { backlog } => Some(backlog),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Domain {
    Inet4,
    Inet6,
    Unix,
}

impl Domain {
    pub(crate) fn for_addr(addr: &net::SocketAddr) -> Self {
        use std::net::SocketAddr;
        match addr {
            SocketAddr::V4(_) => Self::Inet4,
            SocketAddr::V6(_) => Self::Inet6,
        }
    }

    pub(crate) const fn raw(self) -> libc::c_int {
        use libc::{AF_INET, AF_INET6};
        match self {
            Self::Inet4 => AF_INET,
            Self::Inet6 => AF_INET6,
            Self::Unix => libc::AF_UNIX,
        }
    }

    pub(crate) fn from_raw(raw: libc::c_int) -> io::Result<Self> {
        match raw {
            libc::AF_INET => Ok(Self::Inet4),
            libc::AF_INET6 => Ok(Self::Inet6),
            libc::AF_UNIX => Ok(Self::Unix),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported socket address family",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Stream,
    Dgram,
}

impl Kind {
    pub(crate) const fn raw(self) -> libc::c_int {
        use libc::{SOCK_DGRAM, SOCK_STREAM};
        match self {
            Self::Stream => SOCK_STREAM,
            Self::Dgram => SOCK_DGRAM,
        }
    }
}
