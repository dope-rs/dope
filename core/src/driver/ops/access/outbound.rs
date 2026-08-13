use std::process;

use crate::{backend::fixed, driver, io::fd::handles};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Outbound<'d>(driver::Reference<'d>);

impl<'d> Outbound<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(in crate::driver) fn is_unopened(
        self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .is_unopened(key, slot)
    }

    pub(crate) fn begin_outbound_create(
        self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .begin_create(key, slot)
    }

    pub(crate) fn begin_outbound_create_for(self, slot: handles::FixedSlot) -> bool {
        let driver::FixedOwner::Outbound(key) = self.0.files().fixed_owner(slot) else {
            return false;
        };
        self.begin_outbound_create(key, slot)
    }

    pub(crate) fn begin_outbound_close(
        self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> driver::CloseDisposition<'d> {
        let result = self
            .0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .begin_close(key, slot);
        match result {
            Ok((true, None)) => driver::CloseDisposition::Submit(driver::Close::tracked(slot)),
            Ok((false, slots)) => {
                driver::CloseDisposition::NoSubmit(slots.map(driver::RetiredSlots::new))
            }
            Ok((true, Some(_))) | Err(()) => process::abort(),
        }
    }

    pub(crate) fn close_disposition(
        self,
        slot: handles::FixedSlot,
        outbound: Option<driver::OutboundKey>,
    ) -> driver::CloseDisposition<'d> {
        match outbound {
            Some(outbound) => self.begin_outbound_close(outbound, slot),
            None => driver::CloseDisposition::Submit(driver::Close::untracked(slot)),
        }
    }

    pub(crate) fn complete_outbound_close(
        self,
        slot: handles::FixedSlot,
    ) -> Option<driver::RetiredSlots<'d>> {
        let result = self
            .0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .complete_close(slot);
        match result {
            Ok(slots) => slots.map(driver::RetiredSlots::new),
            Err(()) => process::abort(),
        }
    }

    pub(crate) fn release_outbound_slot(
        self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> Option<driver::RetiredSlots<'d>> {
        let result = self
            .0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .release_unopened(key, slot);
        match result {
            Ok(slots) => slots.map(driver::RetiredSlots::new),
            Err(()) => process::abort(),
        }
    }

    pub(crate) fn release_outbound_slot_for(
        self,
        slot: handles::FixedSlot,
    ) -> Option<driver::RetiredSlots<'d>> {
        let driver::FixedOwner::Outbound(key) = self.0.files().fixed_owner(slot) else {
            process::abort();
        };
        self.release_outbound_slot(key, slot)
    }

    pub(crate) fn complete_outbound_create_success(
        self,
        slot: handles::FixedSlot,
    ) -> driver::CreateSuccess<'d> {
        use crate::driver::ops::access::files::leases::CreateTransition;

        let result = self
            .0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .complete_create_success(slot);
        match result {
            Ok(CreateTransition::Deliver(key)) => driver::CreateSuccess::Deliver(key),
            Ok(CreateTransition::Close) => {
                driver::CreateSuccess::Close(driver::Close::tracked(slot))
            }
            Err(()) => process::abort(),
        }
    }

    pub(crate) fn complete_outbound_create_failure(
        self,
        slot: handles::FixedSlot,
    ) -> Option<driver::RetiredSlots<'d>> {
        let result = self
            .0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .complete_create_failure(slot);
        match result {
            Ok(slots) => slots.map(driver::RetiredSlots::new),
            Err(()) => process::abort(),
        }
    }

    pub(crate) fn activate_outbound(
        self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .issues()
            .activate(key, slot)
    }

    pub(crate) fn release_outbound_owner(
        self,
        key: driver::OutboundKey,
    ) -> Option<driver::RetiredSlots<'d>> {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .release_owner(key)
            .map(driver::RetiredSlots::new)
    }

    pub(crate) fn take_retired_slots(
        self,
        retired: driver::RetiredSlots<'d>,
    ) -> fixed::Reservation<'d> {
        self.0
            .shared
            .files
            .leases
            .borrow_mut()
            .take_retired(retired)
    }
}

const _: () = assert!(
    std::mem::size_of::<Outbound<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
