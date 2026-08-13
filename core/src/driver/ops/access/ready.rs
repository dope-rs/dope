use std::io;

use crate::{
    driver::{self, route, schedule::ready},
    io::fd::handles,
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Ready<'d>(driver::Reference<'d>);

impl<'d> Ready<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(in crate::driver) fn arena(self) -> &'d ready::Arena {
        &self.0.shared.scheduling.arena
    }

    pub(crate) fn claim_fixed_ready(self, slot: handles::FixedSlot) -> Option<ready::FixedKey<'d>> {
        self.arena().claim_fixed(slot)
    }

    pub(crate) fn fixed_ready(self, key: ready::FixedKey<'d>) -> ready::Handle<'d> {
        self.arena().fixed_handle(self.0, key)
    }

    pub(crate) fn release_fixed_ready(
        self,
        key: ready::FixedKey<'d>,
    ) -> Option<ready::FixedRelease<'d>> {
        self.arena().release_fixed(key)
    }

    pub fn make_ready_slot<Tag: route::Tag>(
        self,
        target: route::Operation<'d, Tag>,
    ) -> io::Result<ready::Slot<'d, Tag>> {
        self.arena().make_slot(self.0, target)
    }

    pub fn make_ready_slot_reserving<Tag: route::Tag>(
        self,
        target: route::Operation<'d, Tag>,
        reserve: usize,
    ) -> io::Result<ready::Slot<'d, Tag>> {
        self.arena().make_slot_reserving(self.0, target, reserve)
    }

    pub fn make_ready_slots<Tag, I>(self, targets: I) -> io::Result<Box<[ready::Slot<'d, Tag>]>>
    where
        Tag: route::Tag,
        I: IntoIterator<Item = route::Operation<'d, Tag>>,
        I::IntoIter: ExactSizeIterator,
    {
        self.arena().make_slots(self.0, targets)
    }

    pub fn activate_ready(self, key: ready::Key<'d>) {
        ready::Access::with(&self.0, |access| self.arena().activate(access, key));
    }

    pub fn has_ready(self) -> bool {
        self.arena().has_ready()
    }
}

const _: () = assert!(
    std::mem::size_of::<Ready<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
