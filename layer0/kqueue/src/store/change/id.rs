use std::os::fd;

use dope_sys::table::change;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct Id<S> {
    slot: S,
    fd: fd::RawFd,
}

impl<S: Copy + Eq> change::Id for Id<S> {
    type Slot = S;

    fn new(slot: S, fd: fd::RawFd) -> Self {
        Self { slot, fd }
    }

    fn slot(self) -> S {
        self.slot
    }

    fn fd(self) -> fd::RawFd {
        self.fd
    }
}
