mod addr;
mod msg;

use std::net::SocketAddr;

pub use addr::{Addr, InetAddr};
pub use msg::{IoVec, MsgHdr};

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

pub struct Fd<'d> {
    slot: FdSlot,
    driver: &'d crate::Driver,
}

impl std::fmt::Debug for Fd<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Fd").field(&self.slot.0).finish()
    }
}

impl<'d> Fd<'d> {
    #[inline]
    pub fn adopt(slot: FdSlot, driver: &'d crate::Driver) -> Self {
        Self { slot, driver }
    }

    #[inline]
    pub(super) fn slot(&self) -> FdSlot {
        self.slot
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.slot.0
    }
}

impl<'d> Drop for Fd<'d> {
    #[inline]
    fn drop(&mut self) {
        self.driver.release_fd_slot(self.slot);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FdSlot(u32);

impl FdSlot {
    pub fn new(idx: u32) -> Self {
        Self(idx)
    }

    pub fn raw(self) -> u32 {
        self.0
    }
}
