mod addr;
mod msg;

use std::net::SocketAddr;

pub use addr::Addr;
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

use std::ptr::NonNull;

#[derive(Debug)]
pub struct Fd {
    slot: FdSlot,
    driver: NonNull<crate::Driver>,
}

impl Fd {
    pub fn adopt(slot: FdSlot, driver: &mut crate::Driver) -> Self {
        Self {
            slot,
            driver: NonNull::from(driver),
        }
    }

    pub(super) fn slot(&self) -> FdSlot {
        self.slot
    }

    pub fn index(&self) -> u32 {
        self.slot.0
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        // SAFETY: Driver outlives all Fds derived from it (Worker.driver, stable for run_loop; thread-per-core, no aliasing).
        unsafe { self.driver.as_mut() }.release_fd_slot(self.slot);
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
