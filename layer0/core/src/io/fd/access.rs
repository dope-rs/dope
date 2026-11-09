use std::mem;

use o3::depot::arena::slab::Vacancy;

use crate::{
    driver,
    driver::route::{self, indices, kind, table},
    io::fd::{domain, domain::retire, handles, identity, outbound},
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Access<'d>(driver::Reference<'d>);

impl<'d> Access<'d> {
    pub(crate) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn try_reserve<B: route::Tag>(
        &self,
        count: table::Capacity,
    ) -> Result<outbound::Reserved<'d, B>, outbound::Error> {
        domain::Outbound::try_map::<B>(self.0, count)
    }

    pub(crate) fn acquire(
        self,
        key: route::Id,
        local: indices::SlotIndex,
    ) -> Result<handles::SocketSlot<'d>, Vacancy> {
        let id = domain::Outbound::from(self.domain()).acquire(key, local)?;
        Ok(handles::SocketSlot::from_claim(id, self.0))
    }

    pub(crate) fn creation(
        self,
        target: route::Erased<'d, kind::Socket>,
    ) -> Option<domain::Creation> {
        let key = target.route().app()?;
        domain::Outbound::from(self.domain()).creation(key, target.slot())
    }

    pub(crate) fn begin_create(self, slot: handles::OutboundSlot) {
        domain::Outbound::from(self.domain()).begin_create(slot);
    }

    pub(crate) fn complete(self, creation: domain::Creation) -> domain::Create {
        domain::Outbound::from(self.domain()).created(creation)
    }

    pub(crate) fn fail_create(self, creation: domain::Creation) {
        domain::Outbound::from(self.domain()).failed(creation);
    }

    pub(crate) fn activate(self, slot: handles::OutboundSlot) {
        domain::Outbound::from(self.domain()).activate(slot);
    }

    pub(crate) fn retire_route(self, key: route::Id) {
        domain::Outbound::from(self.domain()).retire(key);
    }

    pub(crate) fn retire(self, id: identity::Identity<'d>) {
        let arena = self.0.ready().arena();
        arena.descriptors().retire().retire(id);
        arena.unqueue_fd(&id);
    }

    pub(crate) fn unopened(self, id: identity::Identity<'d, handles::OutboundSlot>) {
        let arena = self.0.ready().arena();
        arena.descriptors().retire().unopened(id);
        arena.unqueue_fd(&id);
    }

    pub(crate) fn creating(self, id: identity::Identity<'d, handles::OutboundSlot>) {
        let arena = self.0.ready().arena();
        arena.descriptors().retire().creating(id);
        arena.unqueue_fd(&id);
    }

    pub(crate) fn set_target(self, id: identity::Identity<'d>, target: route::Token) {
        self.0.ready().arena().descriptors().set_target(id, target);
    }

    pub(crate) fn target(self, id: identity::Identity<'d>) -> Option<route::Token> {
        self.domain().target(id)
    }

    pub(crate) fn abandon(self, accepted: handles::Accepted) -> handles::Reclaimed {
        self.0.ready().arena().unqueue_fd(&accepted);
        self.domain().retire().abandon(accepted)
    }

    pub(crate) fn discard(self, slot: handles::OutboundSlot) {
        self.0.ready().arena().unqueue_fd(&slot);
        self.domain().retire().discard(slot);
    }

    pub(crate) fn retirement(self) -> retire::View<'d> {
        self.domain().retire()
    }

    pub(crate) fn domain(self) -> &'d domain::Domain {
        self.0.ready().arena().descriptors()
    }
}

const _: () = {
    assert!(mem::size_of::<Access<'_>>() == mem::size_of::<driver::Reference<'_>>());
    assert!(mem::align_of::<Access<'_>>() == mem::align_of::<driver::Reference<'_>>());
    assert!(mem::size_of::<domain::Create>() == mem::size_of::<u8>());
    assert!(mem::align_of::<domain::Create>() == mem::align_of::<u8>());
};
