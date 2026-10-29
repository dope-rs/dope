use std::mem;

use o3::mem::{Lease, Return, lease::Open};

use crate::{
    driver,
    io::fd::{domain, slots},
};

/// Exact reason a standalone fixed-file slot could not be reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity { available: u32 },
    Retiring { available: u32 },
}

#[must_use = "a fixed-file slot reservation must be installed or returned"]
pub(crate) struct Reserved<'d> {
    inner: Lease<Release<'d>, Open>,
}

struct Release<'d> {
    driver: driver::Reference<'d>,
}

impl<'d> Return for Release<'d> {
    type Item = slots::ReservedSlot<'d>;

    fn return_item(self, slot: Self::Item) {
        let (identity, solo) = slot.into_inner().release();
        let domain = self.driver.descriptors().domain();
        domain.release(identity);
        domain::Outbound::from(domain).release_one(solo);
    }
}

impl<'d> Reserved<'d> {
    pub(crate) fn try_new(driver: driver::Reference<'d>) -> Result<Self, Error> {
        let solo = domain::Outbound::from(driver.descriptors().domain()).try_reserve()?;
        Ok(Self {
            inner: Lease::open(Release { driver }, slots::ReservedSlot::bind(solo)),
        })
    }

    pub(crate) fn slot(&self) -> &slots::ReservedSlot<'d> {
        self.inner.item()
    }

    pub(crate) fn commit(self) -> (driver::Reference<'d>, slots::ReservedSlot<'d>) {
        let (release, slot) = self.inner.into_parts();
        (release.driver, slot)
    }
}

const _: () = {
    assert!(mem::size_of::<Result<Reserved<'static>, Error>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Reserved<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Reserved<'static>>() == mem::align_of::<usize>());
};
