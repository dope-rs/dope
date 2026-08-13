use std::{io, process};

use crate::{
    backend::fixed,
    driver::{self, route, settings},
    io::fd::handles,
};

pub(in crate::driver::ops::access) struct Leases {
    floor: u32,
    states: [Option<Lease>; route::CAPACITY],
    issued: Box<[Issued]>,
    retiring: u32,
}

pub(in crate::driver::ops::access) struct Issues<'a> {
    leases: &'a mut Leases,
}

struct Lease {
    slots: fixed::Allocation,
    descriptors: u32,
    owner_live: bool,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
/// Packed per-slot owner key and affine-authority/close state.
/// Creation splits one authority into two; activation joins them, while close
/// completion and the last drop may arrive in either order.
struct Issued(u32);

pub(in crate::driver::ops::access) enum CreateTransition {
    Deliver(driver::OutboundKey),
    Close,
}

const _: () = assert!(std::mem::size_of::<Issued>() == std::mem::size_of::<u32>());
const _: () = assert!(route::SLOT_BITS <= u32::BITS - 4);

impl Issued {
    const EMPTY: Self = Self(0);
    const UNOPENED_ONE: u32 = 1;
    const CREATING_ONE: u32 = 2;
    const CREATING_ZERO: u32 = 3;
    const OPEN_ONE: u32 = 4;
    const OPEN_TWO: u32 = 5;
    const FAILED_ONE: u32 = 6;
    const CLOSING_ZERO: u32 = 7;
    const CLOSING_ONE: u32 = 8;
    const CLOSED_ONE: u32 = 9;
    const STATE_SHIFT: u32 = route::SLOT_BITS;
    const KEY_MASK: u32 = route::SLOT_MASK as u32;

    fn new(key: driver::OutboundKey) -> Self {
        Self((Self::UNOPENED_ONE << Self::STATE_SHIFT) | key.raw())
    }

    fn state(self) -> u32 {
        self.0 >> Self::STATE_SHIFT
    }

    fn set_state(&mut self, state: u32) {
        self.0 = (state << Self::STATE_SHIFT) | (self.0 & Self::KEY_MASK);
    }

    fn key(self) -> Option<driver::OutboundKey> {
        if self.0 == 0 {
            return None;
        }
        driver::OutboundKey::from_raw(self.0 & Self::KEY_MASK)
    }
}

impl Leases {
    pub(in crate::driver::ops::access) fn try_new(layout: settings::FileSlots) -> io::Result<Self> {
        use o3::collections::BoxSliceExt;

        let capacity = layout.outbound() as usize;
        Ok(Self {
            floor: layout.accept(),
            states: [const { None }; route::CAPACITY],
            issued: BoxSliceExt::try_box_with(capacity, |_| Issued::EMPTY)?,
            retiring: 0,
        })
    }

    pub(in crate::driver::ops::access) fn issues(&mut self) -> Issues<'_> {
        Issues { leases: self }
    }

    fn relative(&self, raw: u32) -> Option<usize> {
        let index = raw.checked_sub(self.floor)? as usize;
        (index < self.issued.len()).then_some(index)
    }

    pub(in crate::driver::ops::access) fn owner(
        &self,
        slot: handles::FixedSlot,
    ) -> Option<driver::OutboundKey> {
        let index = self.relative(slot.raw())?;
        self.issued.get(index)?.key()
    }

    pub(in crate::driver::ops::access) fn insert<'d, const ID: u8>(
        &mut self,
        slots: fixed::Reservation<'d>,
    ) -> Result<driver::OutboundKey, fixed::Reservation<'d>> {
        if slots.len() == 0 || slots.len() as usize > self.issued.len() {
            return Err(slots);
        }
        let Some(key) = driver::OutboundKey::for_route::<ID>() else {
            return Err(slots);
        };
        let state = &mut self.states[key.raw() as usize];
        if state.is_some() {
            return Err(slots);
        }
        *state = Some(Lease {
            slots: slots.into_allocation(),
            descriptors: 0,
            owner_live: true,
        });
        Ok(key)
    }

    fn get_mut(&mut self, key: driver::OutboundKey) -> Option<&mut Lease> {
        self.states
            .get_mut(key.raw() as usize)
            .and_then(Option::as_mut)
    }

    pub(in crate::driver::ops::access) fn contains(&self, key: driver::OutboundKey) -> bool {
        self.states[key.raw() as usize].is_some()
    }

    pub(in crate::driver::ops::access) fn acquire_descriptor(
        &mut self,
        key: driver::OutboundKey,
        local: route::SlotIndex,
    ) -> Option<handles::FixedSlot> {
        let floor = self.floor;
        let Self { states, issued, .. } = self;
        let lease = states
            .get_mut(key.raw() as usize)
            .and_then(Option::as_mut)?;
        let slot = lease.slots.get(local)?;
        let raw = slot.raw();
        let index = raw.checked_sub(floor)? as usize;
        let issued = issued.get_mut(index)?;
        if issued.0 != 0 {
            return None;
        }
        *issued = Issued::new(key);
        lease.descriptors += 1;
        Some(slot)
    }

    pub(in crate::driver::ops::access) fn physical_index(
        &self,
        key: driver::OutboundKey,
        local: route::SlotIndex,
    ) -> Option<u32> {
        self.states
            .get(key.raw() as usize)?
            .as_ref()?
            .slots
            .get(local)
            .map(handles::FixedSlot::raw)
    }

    pub(in crate::driver::ops::access) fn physical_slot(
        &self,
        key: driver::OutboundKey,
        local: route::SlotIndex,
    ) -> Option<handles::FixedSlot> {
        self.states
            .get(key.raw() as usize)?
            .as_ref()?
            .slots
            .get(local)
    }

    fn validate_issue(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> Option<&mut Issued> {
        let raw = slot.raw();
        let slot_index = self.relative(raw)?;
        let Self { states, issued, .. } = self;
        let lease = states
            .get_mut(key.raw() as usize)
            .and_then(Option::as_mut)?;
        if lease.descriptors == 0 {
            return None;
        }
        let issued = issued.get_mut(slot_index)?;
        if issued.key() != Some(key) {
            return None;
        }
        Some(issued)
    }

    pub(in crate::driver::ops::access) fn release_owner(
        &mut self,
        key: driver::OutboundKey,
    ) -> Option<driver::OutboundKey> {
        let pending = {
            let lease = self.get_mut(key)?;
            if !lease.owner_live {
                return None;
            }
            lease.owner_live = false;
            lease.descriptors != 0
        };
        if pending {
            let Some(retiring) = self.retiring.checked_add(1) else {
                process::abort();
            };
            self.retiring = retiring;
            return None;
        }
        Some(key)
    }

    fn release_issue(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
        expected: u32,
    ) -> Result<Option<driver::OutboundKey>, ()> {
        let raw = slot.raw();
        let issued_index = self.relative(raw).ok_or(())?;
        let issued = self.issued.get_mut(issued_index).ok_or(())?;
        if issued.key() != Some(key) || issued.state() != expected {
            return Err(());
        }
        *issued = Issued::EMPTY;
        {
            let lease = self.get_mut(key).ok_or(())?;
            lease.descriptors = lease.descriptors.checked_sub(1).ok_or(())?;
            if lease.owner_live || lease.descriptors != 0 {
                return Ok(None);
            }
        }
        let Some(retiring) = self.retiring.checked_sub(1) else {
            process::abort();
        };
        self.retiring = retiring;
        Ok(Some(key))
    }

    pub(in crate::driver::ops::access) fn has_retiring(&self) -> bool {
        self.retiring != 0
    }

    pub(in crate::driver::ops::access) fn take_retired<'d>(
        &mut self,
        retired: driver::RetiredSlots<'d>,
    ) -> fixed::Reservation<'d> {
        let key = retired.key();
        let index = key.raw() as usize;
        let Some(state) = self.states.get_mut(index) else {
            process::abort();
        };
        if !state
            .as_ref()
            .is_some_and(|lease| !lease.owner_live && lease.descriptors == 0)
        {
            process::abort();
        }
        let Some(lease) = state.take() else {
            process::abort();
        };
        fixed::Reservation::from_retired(lease.slots, retired)
    }
}

impl Issues<'_> {
    pub(in crate::driver::ops::access) fn begin_create(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        let Some(issued) = self.leases.validate_issue(key, slot) else {
            return false;
        };
        if issued.state() != Issued::UNOPENED_ONE {
            return false;
        }
        issued.set_state(Issued::CREATING_ONE);
        true
    }

    pub(in crate::driver::ops::access) fn is_unopened(
        &self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        let Some(index) = self.leases.relative(slot.raw()) else {
            return false;
        };
        self.leases.issued.get(index).is_some_and(|issued| {
            issued.key() == Some(key) && issued.state() == Issued::UNOPENED_ONE
        })
    }

    pub(in crate::driver::ops::access) fn complete_create_success(
        &mut self,
        slot: handles::FixedSlot,
    ) -> Result<CreateTransition, ()> {
        let index = self.leases.relative(slot.raw()).ok_or(())?;
        let issued = self.leases.issued.get_mut(index).ok_or(())?;
        let key = issued.key().ok_or(())?;
        match issued.state() {
            Issued::CREATING_ONE => {
                issued.set_state(Issued::OPEN_TWO);
                Ok(CreateTransition::Deliver(key))
            }
            Issued::CREATING_ZERO => {
                issued.set_state(Issued::CLOSING_ZERO);
                Ok(CreateTransition::Close)
            }
            _ => Err(()),
        }
    }

    pub(in crate::driver::ops::access) fn complete_create_failure(
        &mut self,
        slot: handles::FixedSlot,
    ) -> Result<Option<driver::OutboundKey>, ()> {
        let index = self.leases.relative(slot.raw()).ok_or(())?;
        let issued = self.leases.issued.get_mut(index).ok_or(())?;
        let key = issued.key().ok_or(())?;
        match issued.state() {
            Issued::CREATING_ONE => {
                issued.set_state(Issued::FAILED_ONE);
                Ok(None)
            }
            Issued::CREATING_ZERO => self.leases.release_issue(key, slot, Issued::CREATING_ZERO),
            _ => Err(()),
        }
    }

    pub(in crate::driver::ops::access) fn activate(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> bool {
        let Some(issued) = self.leases.validate_issue(key, slot) else {
            return false;
        };
        if issued.state() != Issued::OPEN_TWO {
            return false;
        }
        issued.set_state(Issued::OPEN_ONE);
        true
    }

    pub(in crate::driver::ops::access) fn begin_close(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> Result<(bool, Option<driver::OutboundKey>), ()> {
        let issued = self.leases.validate_issue(key, slot).ok_or(())?;
        match issued.state() {
            Issued::CREATING_ONE => {
                issued.set_state(Issued::CREATING_ZERO);
                Ok((false, None))
            }
            Issued::OPEN_ONE => {
                issued.set_state(Issued::CLOSING_ZERO);
                Ok((true, None))
            }
            Issued::OPEN_TWO => {
                issued.set_state(Issued::CLOSING_ONE);
                Ok((true, None))
            }
            Issued::CLOSING_ONE => {
                issued.set_state(Issued::CLOSING_ZERO);
                Ok((false, None))
            }
            Issued::FAILED_ONE => Ok((
                false,
                self.leases.release_issue(key, slot, Issued::FAILED_ONE)?,
            )),
            Issued::CLOSED_ONE => Ok((
                false,
                self.leases.release_issue(key, slot, Issued::CLOSED_ONE)?,
            )),
            _ => Err(()),
        }
    }

    pub(in crate::driver::ops::access) fn complete_close(
        &mut self,
        slot: handles::FixedSlot,
    ) -> Result<Option<driver::OutboundKey>, ()> {
        let Some(index) = self.leases.relative(slot.raw()) else {
            return Ok(None);
        };
        let Some(issued) = self.leases.issued.get_mut(index) else {
            return Ok(None);
        };
        let Some(key) = issued.key() else {
            return Ok(None);
        };
        match issued.state() {
            Issued::CLOSING_ZERO => self.leases.release_issue(key, slot, Issued::CLOSING_ZERO),
            Issued::CLOSING_ONE => {
                issued.set_state(Issued::CLOSED_ONE);
                Ok(None)
            }
            _ => Err(()),
        }
    }

    pub(in crate::driver::ops::access) fn release_unopened(
        &mut self,
        key: driver::OutboundKey,
        slot: handles::FixedSlot,
    ) -> Result<Option<driver::OutboundKey>, ()> {
        let issued = self.leases.validate_issue(key, slot).ok_or(())?;
        if issued.state() != Issued::UNOPENED_ONE {
            return Err(());
        }
        self.leases.release_issue(key, slot, Issued::UNOPENED_ONE)
    }
}
