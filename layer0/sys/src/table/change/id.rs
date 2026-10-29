use std::os::fd;

pub trait Id: Copy + Eq {
    type Slot: Copy + Eq;

    fn new(slot: Self::Slot, fd: fd::RawFd) -> Self;
    fn slot(self) -> Self::Slot;
    fn fd(self) -> fd::RawFd;
}
