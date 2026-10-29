use std::{mem, time};

use crate::kernel;

/// Whether a commit entered the kernel to advance pending work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Progress {
    Idle,
    Entered,
}

/// A duration that cannot be represented by the kernel wait timespec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Overflow(time::Duration);

impl Overflow {
    pub const fn new(span: time::Duration) -> Self {
        Self(span)
    }

    pub const fn requested(self) -> time::Duration {
        self.0
    }
}

/// One affine snapshot of pending submissions and kernel progress flags.
pub trait Enter: Sized {
    type Error;
    type Timeout: TryFrom<time::Duration, Error = Overflow>;

    fn try_submit(self) -> Result<(), Self::Error>;
    fn try_wait(self, timeout: Option<Self::Timeout>) -> Result<(), Self::Error>;
    fn try_commit(self) -> Result<Progress, Self::Error>;
}

/// Exact wait, submit, and cancellation failures retained by a live ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Poll<E, C> {
    Timeout { requested: time::Duration },
    Submit(E),
    Wait(E),
    Cancel(C),
}

impl<E, C> From<Overflow> for Poll<E, C> {
    fn from(overflow: Overflow) -> Self {
        Self::Timeout {
            requested: overflow.requested(),
        }
    }
}

const _: () = {
    type Failure = Poll<kernel::Errno<()>, kernel::Errno<()>>;
    assert!(mem::size_of::<Failure>() == mem::size_of::<time::Duration>());
    assert!(mem::align_of::<Failure>() == mem::align_of::<time::Duration>());
    assert!(mem::size_of::<Result<(), Failure>>() == mem::size_of::<Failure>());
};
