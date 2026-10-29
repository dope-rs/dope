use std::mem;

use crate::{
    driver,
    driver::route,
    io::fd::{domain, domain::retire, handles, identity},
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Access<'d>(driver::Reference<'d>);

impl<'d> Access<'d> {
    pub(crate) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
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
};
