use std::{cell, fmt};

use crate::driver::{self, schedule::ready};

pub mod raw;

/// Linear, driver-scoped wake target for an in-flight operation.
/// It preserves the registered child target, and [`wake`](Self::wake) consumes it.
#[repr(transparent)]
pub struct Waker<'d>(raw::Waker<'d>);

#[doc(hidden)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Wake<'d>(pub(in crate::driver::schedule::ready) raw::Waker<'d>);

impl PartialEq for Wake<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.same_target(&other.0)
    }
}

impl Eq for Wake<'_> {}

impl fmt::Debug for Waker<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waker").finish_non_exhaustive()
    }
}

/// Owns a linear [`Waker`] without clearing it on the hot wake path.
#[doc(hidden)]
#[repr(transparent)]
pub struct Slot<'d> {
    wake: cell::Cell<Option<raw::Waker<'d>>>,
}

impl<'d> Waker<'d> {
    pub fn from_ready(driver: driver::Reference<'d>, key: ready::Key<'d>) -> Self {
        Self(raw::Waker::from_ready(driver, key))
    }

    pub fn wake(self) {
        self.0.wake();
    }
}

impl<'d> Wake<'d> {
    pub fn from_ready(driver: driver::Reference<'d>, key: ready::Key<'d>) -> Self {
        Self::from(ready::Target::new(driver, key))
    }

    pub fn completion(self) -> Waker<'d> {
        Waker(self.0)
    }

    pub fn wake(self) {
        self.0.wake();
    }

    pub(in crate::driver::schedule::ready) fn same_driver(self, other: Self) -> bool {
        self.0.same_driver(&other.0)
    }
}

impl<'d> From<ready::Target<'d>> for Wake<'d> {
    fn from(target: ready::Target<'d>) -> Self {
        let (driver, key) = target.into_parts();
        Self(raw::Waker::from_ready(driver, key))
    }
}

impl<'d> Slot<'d> {
    pub const fn empty() -> Self {
        use std::cell::Cell;
        Self {
            wake: Cell::new(None),
        }
    }

    pub fn set(&self, wake: Waker<'d>) {
        self.wake.set(Some(wake.0));
    }

    pub fn clear(&self) {
        self.wake.set(None);
    }

    pub fn is_empty(&self) -> bool {
        self.wake.get().is_none()
    }

    pub fn take(&self) -> Option<Waker<'d>> {
        self.wake.take().map(Waker)
    }

    pub fn wake(&self) {
        if let Some(wake) = self.wake.get() {
            wake.wake();
        }
    }
}
