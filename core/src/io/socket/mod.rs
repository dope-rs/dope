pub mod addr;
pub mod msg;

use core::mem::MaybeUninit;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug)]
pub struct ListenerConfig {
    pub reuse_addr: bool,
    pub reuse_port: bool,
    pub fast_open_backlog: Option<u32>,
    pub defer_accept_secs: Option<u32>,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            reuse_addr: true,
            reuse_port: false,
            fast_open_backlog: None,
            defer_accept_secs: None,
        }
    }
}

impl ListenerConfig {
    pub fn for_datagram(addr: &SocketAddr) -> Self {
        let reuse = addr.port() != 0;
        Self {
            reuse_addr: reuse,
            reuse_port: reuse,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Domain {
    Inet4,
    Inet6,
}

impl Domain {
    pub fn for_addr(addr: &SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(_) => Self::Inet4,
            SocketAddr::V6(_) => Self::Inet6,
        }
    }

    pub const fn raw(self) -> libc::c_int {
        match self {
            Self::Inet4 => libc::AF_INET,
            Self::Inet6 => libc::AF_INET6,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Stream,
    Dgram,
}

impl Kind {
    pub const fn raw(self) -> libc::c_int {
        match self {
            Self::Stream => libc::SOCK_STREAM,
            Self::Dgram => libc::SOCK_DGRAM,
        }
    }
}

/// # Safety
/// Implementors must be valid when every byte is zero.
pub(crate) unsafe trait Pod: Sized {
    fn zeroed() -> Self {
        unsafe { MaybeUninit::<Self>::zeroed().assume_init() }
    }
}

unsafe impl Pod for libc::sockaddr_in {}
unsafe impl Pod for libc::sockaddr_in6 {}
unsafe impl Pod for libc::sockaddr_un {}
unsafe impl Pod for libc::sockaddr_storage {}
