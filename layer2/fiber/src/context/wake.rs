use ready::task;

use crate::core::driver::{
    self,
    schedule::ready::{self, wake},
};

/// Driver-owned wake target that cannot name a task node.
/// The root boundary supplies it, and child contexts retain that exact root.
/// Generational ready keys make wakes after slot release a no-op.
///
/// ```compile_fail
/// use dope_fiber::context::wake::RootWaker;
///
/// fn retag<'left, 'right>(wake: RootWaker<'left>) -> RootWaker<'right> {
///     wake
/// }
/// ```
#[derive(Clone, Copy)]
pub struct RootWaker<'d> {
    target: ready::Target<'d>,
}

impl<'d> RootWaker<'d> {
    pub fn wake(self) {
        self.target.wake();
    }
}

impl<'d> From<ready::Target<'d>> for RootWaker<'d> {
    fn from(target: ready::Target<'d>) -> Self {
        Self { target }
    }
}

impl<'d> From<RootWaker<'d>> for Waker<'d> {
    fn from(root: RootWaker<'d>) -> Self {
        Waker::from_parent(task::Parent::from(root.target))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Waker<'d>(pub(crate) task::Parent<'d>);

impl<'d> Waker<'d> {
    pub(crate) fn from_parent(parent: task::Parent<'d>) -> Self {
        Self(parent)
    }

    /// Returns the scheduler wake capability carried by this fiber wake.
    pub fn completion(self) -> wake::Waker<'d> {
        self.0.into()
    }

    pub fn wake(self) {
        self.0.wake();
    }

    pub fn belongs_to(self, driver: driver::Reference<'_>) -> bool {
        self.0.belongs_to(driver)
    }
}
