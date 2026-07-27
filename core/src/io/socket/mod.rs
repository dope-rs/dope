pub mod addr;
pub mod msg;
pub mod option;

use core::mem::MaybeUninit;
use libc::AF_INET;
use libc::AF_INET6;
use libc::SOCK_DGRAM;
use libc::SOCK_STREAM;
use libc::c_int;
use libc::sockaddr_in;
use libc::sockaddr_in6;
use libc::sockaddr_storage;
use libc::sockaddr_un;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug)]
pub struct ListenerConfig {
    pub reuse_addr: bool,
    pub reuse_port: bool,
    pub fast_open_backlog: Option<c_int>,
    pub defer_accept_secs: Option<c_int>,
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

    pub const fn raw(self) -> c_int {
        match self {
            Self::Inet4 => AF_INET,
            Self::Inet6 => AF_INET6,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Stream,
    Dgram,
}

impl Kind {
    pub const fn raw(self) -> c_int {
        match self {
            Self::Stream => SOCK_STREAM,
            Self::Dgram => SOCK_DGRAM,
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

unsafe impl Pod for sockaddr_in {}
unsafe impl Pod for sockaddr_in6 {}
unsafe impl Pod for sockaddr_un {}
unsafe impl Pod for sockaddr_storage {}
