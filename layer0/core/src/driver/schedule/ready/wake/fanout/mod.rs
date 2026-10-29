pub mod storage;
pub use storage::Reject as Error;

use std::mem;

use o3::depot::queue::resident;

use crate::{
    driver,
    driver::schedule::{
        app,
        ready::{self, wake, wake::resident::drain},
    },
};

/// Linear ownership of one persistent subscription slot.
#[repr(transparent)]
pub struct Registration<'target, 'd: 'target> {
    inner: storage::Lease<'target, ready::Retained<'d>>,
}

/// Fixed-capacity set of persistent wake subscriptions.
pub struct Set<'d> {
    driver: driver::Reference<'d>,
    inner: storage::Registry<ready::Retained<'d>>,
}

impl<'d> Set<'d> {
    pub fn try_new(
        driver: driver::Reference<'d>,
        capacity: usize,
    ) -> Result<Self, resident::Error> {
        Ok(Self {
            driver,
            inner: storage::Registry::try_new(capacity)?,
        })
    }

    pub fn try_register<'target>(
        &'target self,
        lease: &mut Registration<'target, 'd>,
        wake: &wake::Waker<'d>,
    ) -> Result<(), Error> {
        self.inner.try_keep(&mut lease.inner, *wake.key())
    }

    pub fn publish(&self) {
        self.inner.publish();
    }

    pub fn has_pending(&self) -> bool {
        self.inner.has_pending()
    }

    pub fn drain_one(&self, work: app::Work<'_, 'd>) -> wake::Drain {
        drain::one(
            self.driver,
            work,
            || self.inner.has_pending(),
            || self.inner.step(),
        )
    }
}

impl Default for Registration<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl Registration<'_, '_> {
    pub const fn new() -> Self {
        Self {
            inner: storage::Lease::new(),
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }
}

const _: () = {
    assert!(mem::size_of::<Registration<'static, 'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Registration<'static, 'static>>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Set<'static>>()
            == mem::size_of::<(
                driver::Reference<'static>,
                storage::Registry<ready::Retained<'static>>,
            )>()
    );
};
