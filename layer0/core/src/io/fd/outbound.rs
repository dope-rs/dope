use core::marker::PhantomData;
use std::mem;

use o3::{
    depot::arena::slab::Vacancy,
    mem::{Lease, Return},
};

use crate::{
    driver,
    driver::route::{self, table},
    io::fd::handles,
};

/// Exact reason a driver-local outbound descriptor range was not reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    Occupied,
    Capacity { requested: usize, available: usize },
    Retiring { requested: usize, available: usize },
}

/// Linear authority over one route's driver-local descriptor range.
pub struct Reserved<'d, B: route::Tag> {
    inner: Lease<Release<'d, B>>,
}

struct Release<'d, B: route::Tag> {
    brand: PhantomData<B>,
    driver: driver::Reference<'d>,
}

impl<B: route::Tag> Return for Release<'_, B> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.driver
            .descriptors()
            .retire_route(route::Id::for_tag::<B>());
    }
}

impl<'d, B: route::Tag> Reserved<'d, B> {
    /// The framework route cannot reserve an application descriptor range.
    pub fn try_new(
        driver: &driver::Context<'_, 'd>,
        count: table::Capacity,
    ) -> Result<Self, Error> {
        driver.driver_ref().descriptors().try_reserve::<B>(count)
    }

    pub(crate) fn new(driver: driver::Reference<'d>) -> Self {
        Self {
            inner: Lease::new(
                Release {
                    driver,
                    brand: PhantomData,
                },
                (),
            ),
        }
    }

    /// Binds one local pool target to its physical descriptor slot.
    ///
    /// A target from another route cannot acquire this reservation.
    /// Unavailable slots report `Full`; permanently consumed descriptor generations
    /// report `IdentityExhausted`.
    pub fn bind(
        &self,
        target: route::Target<'d, B>,
    ) -> Result<route::Bound<'d, B, handles::SocketSlot<'d>>, Vacancy> {
        let release = self.inner.sink();
        let key = route::Id::for_tag::<B>();
        let slot = release.driver.descriptors().acquire(key, target.slot())?;
        Ok(target.bind(slot))
    }
}

const _: () = {
    assert!(
        mem::size_of::<Reserved<'_, route::KeyTag<0>>>() == mem::size_of::<driver::Reference<'_>>()
    );
    assert!(
        mem::align_of::<Reserved<'_, route::KeyTag<0>>>()
            == mem::align_of::<driver::Reference<'_>>()
    );
};
