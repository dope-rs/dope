use std::io;

use crate::{
    backend::fixed,
    driver::{self, schedule::ready, settings, storage::retirements},
    io::fd::handles,
};

pub(super) struct State {
    retirements: retirements::Queue,
}

impl State {
    pub(super) fn try_new(file_slots: settings::FileSlots) -> io::Result<Self> {
        Ok(Self {
            retirements: retirements::Queue::try_new(file_slots)?,
        })
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Maintenance<'d>(driver::Reference<'d>);

impl<'d> Maintenance<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn has_deferred_maintenance(self) -> bool {
        !self.0.shared.receive.returned.is_empty()
            || !self.0.shared.maintenance.retirements.is_empty()
    }

    pub(crate) fn return_buffer(self, buffer: driver::Buffer) {
        self.0.shared.receive.returned.push(buffer);
    }

    pub(crate) fn pop_returned_buffer(self) -> Option<driver::Buffer> {
        self.0.shared.receive.returned.pop()
    }

    pub(crate) fn defer_route(self, id: u8) {
        self.0
            .shared
            .maintenance
            .retirements
            .push(retirements::Record::Route(id));
    }

    pub(crate) fn defer_descriptor(
        self,
        slot: handles::FixedSlot,
        outbound: Option<driver::OutboundKey>,
    ) {
        self.0
            .shared
            .maintenance
            .retirements
            .push(retirements::Record::Descriptor { slot, outbound });
    }

    pub(crate) fn defer_fixed_slot(self, slot: fixed::Retirement<'d>) {
        self.0
            .shared
            .maintenance
            .retirements
            .push(retirements::Record::Retire(slot));
    }

    pub(crate) fn defer_outbound_slots(self, slots: driver::RetiredSlots<'d>) {
        self.0
            .shared
            .maintenance
            .retirements
            .push(retirements::Record::OutboundSlots { slots });
    }

    pub(crate) fn defer_close(self, close: driver::Close<'d>) {
        self.0
            .shared
            .maintenance
            .retirements
            .push(retirements::Record::Close {
                slot: close.into_slot(),
            });
    }

    pub(in crate::driver) fn pop_deferred_retirement(self) -> Option<retirements::Record<'d>> {
        self.0.shared.maintenance.retirements.pop(self.0)
    }

    pub(crate) fn retire_fixed_release(self, released: ready::FixedRelease<'d>) {
        let slot = released.slot();
        match self.0.files().fixed_owner(slot) {
            driver::FixedOwner::Accepted => self.defer_descriptor(released.into_slot(), None),
            driver::FixedOwner::Reserved => {
                self.defer_fixed_slot(fixed::Retirement::from_release(released));
            }
            driver::FixedOwner::Outbound(outbound) => {
                if !self.0.outbound().is_unopened(outbound, slot) {
                    self.defer_descriptor(released.into_slot(), Some(outbound));
                    return;
                }
                if let Some(slots) = self
                    .0
                    .outbound()
                    .release_outbound_slot(outbound, released.into_slot())
                {
                    self.defer_outbound_slots(slots);
                }
            }
        }
    }
}

const _: () = assert!(
    std::mem::size_of::<Maintenance<'static>>()
        == std::mem::size_of::<driver::Reference<'static>>()
);
