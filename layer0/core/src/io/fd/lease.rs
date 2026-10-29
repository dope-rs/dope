use std::marker;

use o3::{mem, mem::lease};

use crate::{
    driver,
    driver::schedule::ready,
    io::fd::{handles, identity, slots},
};

#[repr(transparent)]
pub(super) struct Lease<'d, P: Phase>
where
    Retire<'d, P>: mem::Return<Item = P::Id<'d>>,
{
    inner: mem::Lease<Retire<'d, P>, lease::Open>,
}

pub(super) enum Active {}
pub(super) enum Creating {}
pub(super) enum Unopened {}

pub(super) trait Phase {
    type Id<'d>: Copy;
}

pub(super) struct Retire<'d, P> {
    driver: driver::Reference<'d>,
    phase: marker::PhantomData<P>,
}

impl<'d> mem::Return for Retire<'d, Active> {
    type Item = identity::Identity<'d>;

    fn return_item(self, id: Self::Item) {
        self.driver.descriptors().retire(id);
    }
}

impl Phase for Active {
    type Id<'d> = identity::Identity<'d>;
}

impl<'d> mem::Return for Retire<'d, Creating> {
    type Item = identity::Identity<'d, handles::OutboundSlot>;

    fn return_item(self, id: Self::Item) {
        self.driver.descriptors().creating(id);
    }
}

impl Phase for Creating {
    type Id<'d> = identity::Identity<'d, handles::OutboundSlot>;
}

impl<'d> mem::Return for Retire<'d, Unopened> {
    type Item = identity::Identity<'d, handles::OutboundSlot>;

    fn return_item(self, id: Self::Item) {
        self.driver.descriptors().unopened(id);
    }
}

impl Phase for Unopened {
    type Id<'d> = identity::Identity<'d, handles::OutboundSlot>;
}

impl<'d, P: Phase> Lease<'d, P>
where
    Retire<'d, P>: mem::Return<Item = P::Id<'d>>,
{
    fn new(driver: driver::Reference<'d>, id: P::Id<'d>) -> Self {
        Self {
            inner: mem::Lease::open(
                Retire {
                    driver,
                    phase: marker::PhantomData,
                },
                id,
            ),
        }
    }

    pub(super) fn driver(&self) -> driver::Reference<'d> {
        self.inner.sink().driver
    }
}

impl<'d> Lease<'d, Active> {
    pub(super) fn from_claim(id: identity::Identity<'d>, driver: driver::Reference<'d>) -> Self {
        Self::new(driver, id)
    }

    pub(super) fn from_slot(slot: slots::ReservedSlot<'d>, driver: driver::Reference<'d>) -> Self {
        Self::new(driver, slot.into_inner().into_id())
    }

    pub(super) fn slot_ref(&self) -> &handles::PhysicalSlot {
        self.inner.item().slot_ref()
    }

    pub(super) fn slot(&self) -> handles::PhysicalSlot {
        self.inner.item().slot()
    }

    pub(super) fn ready_handle(&self) -> ready::Handle<'d> {
        self.driver().ready().handle(*self.inner.item())
    }
}

impl<'d> Lease<'d, Unopened> {
    pub(super) fn from_claim(
        id: identity::Identity<'d, handles::OutboundSlot>,
        driver: driver::Reference<'d>,
    ) -> Self {
        Self::new(driver, id)
    }

    pub(super) fn begin(self) -> Lease<'d, Creating> {
        let (sink, id) = self.inner.into_parts();
        sink.driver.outbound().begin_create(id.slot());
        Lease::new(sink.driver, id)
    }

    pub(super) fn outbound(&self) -> handles::OutboundSlot {
        self.inner.item().slot()
    }
}

impl<'d> Lease<'d, Creating> {
    pub(super) fn activate(self) -> Lease<'d, Active> {
        let (sink, id) = self.inner.into_parts();
        Lease::new(sink.driver, id.physical())
    }

    pub(super) fn ready_handle(&self) -> ready::Handle<'d> {
        self.driver().ready().handle(self.inner.item().physical())
    }

    pub(super) fn slot(&self) -> handles::PhysicalSlot {
        self.inner.item().slot().physical()
    }
}

const _: () = {
    use std::mem::{align_of, size_of};

    assert!(
        size_of::<Lease<'static, Active>>()
            == size_of::<(driver::Reference<'static>, identity::Identity<'static>)>()
    );
    assert!(
        align_of::<Lease<'static, Active>>()
            == align_of::<(driver::Reference<'static>, identity::Identity<'static>)>()
    );
};
