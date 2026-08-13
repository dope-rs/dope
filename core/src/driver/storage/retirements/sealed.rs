use std::{cell, io, process};

use o3::collections::batch::set;

use crate::{
    backend::fixed,
    driver::{self, route, settings, storage::retirements},
    io::fd::handles,
};

const ACTION_BITS: u32 = route::SLOT_BITS + 3;
const ACTION_MASK: u64 = (1 << ACTION_BITS) - 1;
const CLOSE: u64 = 1;
const RETIRE_SLOT: u64 = 2;
const DESCRIPTOR: u64 = 3;
const OUTBOUND: u64 = 4;

/// Coalesced retirement work indexed by its consumed authority.
/// Socket creation may split one authority, so a slot retains two actions.
pub(in crate::driver) struct Queue {
    slot_actions: Box<[cell::Cell<u64>]>,
    slots: set::Set<handles::FixedSlot>,
    owners: set::Set<driver::OutboundKey>,
    routes: set::Set<u8>,
}

impl Queue {
    pub(in crate::driver) fn try_new(file_slots: settings::FileSlots) -> io::Result<Self> {
        use o3::collections::BoxSliceExt;

        let capacity = file_slots.capacity() as usize;
        Ok(Self {
            slot_actions: BoxSliceExt::try_box_with(capacity, |_| cell::Cell::new(0))?,
            slots: set::Set::try_with_capacity(capacity)?,
            owners: set::Set::try_with_capacity(route::CAPACITY)?,
            routes: set::Set::try_with_capacity(route::CAPACITY)?,
        })
    }

    pub(in crate::driver) fn push(&self, record: retirements::Record<'_>) {
        match record {
            retirements::Record::Route(id) => {
                if !self.routes.insert(id) {
                    process::abort();
                }
            }
            retirements::Record::OutboundSlots { slots } => {
                let owner = slots.into_key();
                if !self.owners.insert(owner) {
                    process::abort();
                }
            }
            retirements::Record::Close { slot } => self.push_slot(slot, CLOSE),
            retirements::Record::Retire(retired) => {
                self.push_slot(retired.into_raw().into_fixed(), RETIRE_SLOT);
            }
            retirements::Record::Descriptor { slot, outbound } => {
                let action = match outbound {
                    None => DESCRIPTOR,
                    Some(owner) => (u64::from(owner.raw()) << 3) | OUTBOUND,
                };
                self.push_slot(slot, action);
            }
        }
    }

    pub(in crate::driver) fn pop<'d>(
        &self,
        driver: driver::Reference<'d>,
    ) -> Option<retirements::Record<'d>> {
        if let Some(slot) = self.slots.pop() {
            return Some(self.pop_slot(slot, driver));
        }
        if let Some(owner) = self.owners.pop() {
            return Some(retirements::Record::OutboundSlots {
                slots: driver::RetiredSlots::new(owner),
            });
        }
        self.routes.pop().map(retirements::Record::Route)
    }

    pub(in crate::driver) fn is_empty(&self) -> bool {
        self.slots.is_empty() && self.owners.is_empty() && self.routes.is_empty()
    }

    fn push_slot(&self, slot: handles::FixedSlot, action: u64) {
        debug_assert!(action != 0 && action <= ACTION_MASK);
        let Some(cell) = self.slot_actions.get(slot.raw() as usize) else {
            process::abort();
        };
        let current = cell.get();
        if current == 0 {
            cell.set(action);
            if !self.slots.insert(slot) {
                process::abort();
            }
            return;
        }
        if current >> ACTION_BITS != 0 {
            process::abort();
        }
        cell.set(current | (action << ACTION_BITS));
    }

    fn pop_slot<'d>(
        &self,
        slot: handles::FixedSlot,
        _driver: driver::Reference<'d>,
    ) -> retirements::Record<'d> {
        let index = slot.raw() as usize;
        let cell = &self.slot_actions[index];
        let actions = cell.get();
        let action = actions & ACTION_MASK;
        let remaining = actions >> ACTION_BITS;
        if action == 0 {
            process::abort();
        }
        cell.set(remaining);
        if remaining != 0 && !self.slots.insert(slot) {
            process::abort();
        }
        match action & 0b111 {
            CLOSE => retirements::Record::Close { slot },
            RETIRE_SLOT => {
                // SAFETY: this action is inserted only by consuming one
                // `fixed::Slot`; removing its unique bitmap bit restores it.
                let retired = unsafe { fixed::raw::Retirement::from_deferred(slot) };
                retirements::Record::Retire(retired.bind(_driver))
            }
            DESCRIPTOR => retirements::Record::Descriptor {
                slot,
                outbound: None,
            },
            OUTBOUND => {
                let owner = (action >> 3) as u8;
                debug_assert!(owner < route::FRAMEWORK);
                let owner = driver::OutboundKey::from_bounded(owner);
                retirements::Record::Descriptor {
                    slot,
                    outbound: Some(owner),
                }
            }
            _ => process::abort(),
        }
    }
}
