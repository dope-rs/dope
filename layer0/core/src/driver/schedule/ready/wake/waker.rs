use std::mem;

use ready::task;

use crate::driver::schedule::ready;

/// A wake capability preserves its invariant driver scope through registration.
///
///
#[repr(transparent)]
#[derive(PartialEq, Eq)]
pub struct Waker<'d>(pub(super) ready::Address<'d>);

impl<'d> Waker<'d> {
    pub const fn key(&self) -> &ready::Retained<'d> {
        &self.0.key
    }

    pub fn wake(self) {
        self.0.wake();
    }
}

impl<'d> From<ready::Target<'d>> for Waker<'d> {
    fn from(target: ready::Target<'d>) -> Self {
        Self(target.address())
    }
}

impl<'d> From<task::Parent<'d>> for Waker<'d> {
    fn from(parent: task::Parent<'d>) -> Self {
        Self(parent.address())
    }
}

const _: () = {
    assert!(mem::size_of::<Waker<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Waker<'static>>>() == mem::size_of::<Waker<'static>>());
};
