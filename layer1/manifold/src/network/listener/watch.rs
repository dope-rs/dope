use std::{cell::Cell, task::Poll};

use dope_core::driver::schedule::ready::wake;
use dope_runtime::executor::root;

use super::AcceptError;

pub struct Watch<'d> {
    error: Cell<Option<AcceptError>>,
    wake: wake::store::Slot<'d>,
}

#[must_use]
pub struct Wait<'a, 'd>(&'a Watch<'d>);

impl<'d> Watch<'d> {
    pub(super) const fn new() -> Self {
        Self {
            error: Cell::new(None),
            wake: wake::store::Slot::empty(),
        }
    }

    pub(super) fn record(&self, cause: AcceptError) {
        if self.error.get().is_none() {
            self.error.set(Some(cause));
            self.wake.wake_once();
        }
    }

    pub fn error(&self) -> Option<AcceptError> {
        self.error.get()
    }
}

impl<'a, 'd> Wait<'a, 'd> {
    pub(super) const fn new(watch: &'a Watch<'d>) -> Self {
        Self(watch)
    }

    pub const fn watch(&self) -> &'a Watch<'d> {
        self.0
    }

    pub fn poll(&mut self, wake: wake::Waker<'d>) -> Poll<AcceptError> {
        match self.0.error() {
            Some(cause) => Poll::Ready(cause),
            None => {
                self.0.wake.set(wake);
                Poll::Pending
            }
        }
    }
}

impl<'d> root::Root<'d> for Wait<'_, 'd> {
    type Output = AcceptError;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, target, _, _) = context.into_parts();
        root.get_mut().poll(wake::Waker::from(target))
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        self.0.wake.clear();
    }
}
