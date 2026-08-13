use std::{cell, io};

use crate::{
    backend::fixed,
    driver::{self, route, settings},
    io::fd::handles,
};

pub(super) mod leases;

pub(super) struct State {
    pub(super) leases: cell::RefCell<leases::Leases>,
    accept_slots: usize,
    outbound_slots: usize,
}

impl State {
    pub(super) fn try_new(layout: settings::FileSlots) -> io::Result<Self> {
        Ok(Self {
            leases: cell::RefCell::new(leases::Leases::try_new(layout)?),
            accept_slots: layout.accept_capacity().get(),
            outbound_slots: layout.outbound() as usize,
        })
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Files<'d>(driver::Reference<'d>);

impl<'d> Files<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn accept_capacity(self) -> usize {
        self.0.shared.files.accept_slots
    }

    pub(crate) fn outbound_capacity(self) -> usize {
        self.0.shared.files.outbound_slots
    }

    pub(crate) fn fixed_owner(self, slot: handles::FixedSlot) -> driver::FixedOwner {
        if slot.raw() < self.0.shared.files.accept_slots as u32 {
            return driver::FixedOwner::Accepted;
        }
        match self.0.shared.files.leases.borrow().owner(slot) {
            Some(owner) => driver::FixedOwner::Outbound(owner),
            None => driver::FixedOwner::Reserved,
        }
    }

    pub(crate) fn track_outbound_slots<const ID: u8>(
        self,
        slots: fixed::Reservation<'d>,
    ) -> Result<driver::OutboundKey, fixed::Reservation<'d>> {
        self.0.shared.files.leases.borrow_mut().insert::<ID>(slots)
    }

    pub(crate) fn has_outbound_route<const ID: u8>(self) -> bool {
        driver::OutboundKey::for_route::<ID>()
            .is_some_and(|key| self.0.shared.files.leases.borrow().contains(key))
    }

    pub(crate) fn acquire_outbound_descriptor(
        self,
        key: driver::OutboundKey,
        local: route::SlotIndex,
    ) -> Option<handles::FixedSlot> {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .acquire_descriptor(key, local)
    }

    pub(crate) fn outbound_physical_index(
        self,
        key: driver::OutboundKey,
        local: route::SlotIndex,
    ) -> Option<u32> {
        self.0
            .shared
            .files
            .leases
            .borrow()
            .physical_index(key, local)
    }

    pub(crate) fn outbound_slot_for_target(
        self,
        target: route::Token,
    ) -> Option<handles::FixedSlot> {
        let key = driver::OutboundKey::from_raw(u32::from(target.route()))?;
        self.0
            .shared
            .files
            .leases
            .borrow()
            .physical_slot(key, target.slot())
    }

    pub(crate) fn has_retiring_outbound(self) -> bool {
        self.0.shared.files.leases.borrow().has_retiring()
    }

    pub(crate) fn map_fixed_allocation_error(self, error: io::Error) -> io::Error {
        if error.kind() == io::ErrorKind::OutOfMemory && self.has_retiring_outbound() {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "dope: fixed-file slots are still retiring",
            )
        } else {
            error
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Files<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
