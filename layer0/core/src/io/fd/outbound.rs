use core::marker::PhantomData;
use std::mem;

use o3::mem::{Lease, Return};

use crate::{
    driver,
    driver::{
        route,
        route::{indices, kind, table},
    },
    io::fd::{domain, handles},
};

fn descriptors(driver: driver::Reference<'_>) -> &domain::Domain {
    driver.ready().arena().descriptors()
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Context<'d>(driver::Reference<'d>);

impl<'d> Context<'d> {
    pub(crate) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn try_reserve<B: route::Tag>(
        &self,
        count: table::Capacity,
    ) -> Result<Reserved<'d, B>, Error> {
        domain::Outbound::try_map::<B>(self.0, count)
    }

    pub(crate) fn acquire(
        self,
        key: route::OutboundKey,
        local: indices::SlotIndex,
    ) -> Option<handles::SocketSlot<'d>> {
        let id = domain::Outbound::from(descriptors(self.0)).acquire(key, local)?;
        Some(handles::SocketSlot::from_claim(id, self.0))
    }

    pub(crate) fn creation(
        self,
        target: route::Erased<'d, kind::Socket>,
    ) -> Option<domain::Creation> {
        let key = route::OutboundKey::from_raw(u32::from(target.route().app()?.raw()))?;
        domain::Outbound::from(descriptors(self.0)).creation(key, target.slot())
    }

    pub(crate) fn begin_create(self, slot: handles::OutboundSlot) {
        domain::Outbound::from(descriptors(self.0)).begin_create(slot);
    }

    pub(crate) fn complete(self, creation: domain::Creation) -> domain::Create {
        domain::Outbound::from(descriptors(self.0)).created(creation)
    }

    pub(crate) fn fail_create(self, creation: domain::Creation) {
        domain::Outbound::from(descriptors(self.0)).failed(creation);
    }

    pub(crate) fn activate(self, slot: handles::OutboundSlot) {
        domain::Outbound::from(descriptors(self.0)).activate(slot);
    }

    pub(crate) fn retire(self, key: route::OutboundKey) {
        domain::Outbound::from(descriptors(self.0)).retire(key);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Context<'_>>() == std::mem::size_of::<driver::Reference<'_>>());
    assert!(std::mem::align_of::<Context<'_>>() == std::mem::align_of::<driver::Reference<'_>>());
    assert!(std::mem::size_of::<domain::Create>() == std::mem::size_of::<u8>());
    assert!(std::mem::align_of::<domain::Create>() == std::mem::align_of::<u8>());
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
            .outbound()
            .retire(route::OutboundKey::for_tag::<B>());
    }
}

impl<'d, B: route::Tag> Reserved<'d, B> {
    /// The framework route cannot reserve an application descriptor range.
    pub fn try_new(
        driver: &driver::Context<'_, 'd>,
        count: table::Capacity,
    ) -> Result<Self, Error> {
        driver.driver_ref().outbound().try_reserve::<B>(count)
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
    /// A target from another route cannot acquire this reservation:
    ///
    pub fn bind(
        &self,
        target: route::Target<'d, B>,
    ) -> Option<route::Bound<'d, B, handles::SocketSlot<'d>>> {
        let release = self.inner.sink();
        let key = route::OutboundKey::for_tag::<B>();
        let slot = release.driver.outbound().acquire(key, target.slot())?;
        Some(target.bind(slot))
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
