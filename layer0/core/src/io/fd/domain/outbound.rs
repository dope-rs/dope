use std::{cell, mem};

use crate::{
    driver,
    driver::{
        route,
        route::{indices, indices::SlotIndex, table::Capacity},
    },
    io::{
        fd::{
            domain::{Domain, Lease, Prefix, State, resident, state},
            handles, identity, outbound,
            outbound::Error,
            states,
        },
        socket::install::slot,
    },
};

pub(crate) enum Create {
    Deliver,
    Retired,
}

pub(crate) enum Close {
    Submit,
    Settled,
    Released,
}

#[must_use = "a bound socket creation completion must succeed or fail"]
#[repr(transparent)]
pub(crate) struct Creation(handles::OutboundSlot);

impl AsRef<handles::OutboundSlot> for Creation {
    fn as_ref(&self) -> &handles::OutboundSlot {
        &self.0
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Outbound<'a>(pub(super) &'a Domain);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Closures<'a>(&'a Domain);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Index(pub(super) indices::SlotIndex);

#[must_use = "a standalone slot must remain owned until it is returned to its partition"]
#[repr(transparent)]
pub(crate) struct Solo<T = handles::PhysicalSlot>(pub(super) T);

impl<'a> From<&'a Domain> for Outbound<'a> {
    fn from(domain: &'a Domain) -> Self {
        Self(domain)
    }
}

impl<'a> From<&'a Domain> for Closures<'a> {
    fn from(domain: &'a Domain) -> Self {
        Self(domain)
    }
}

impl<'d> Solo<identity::Identity<'d>> {
    pub(crate) fn physical(&self) -> handles::PhysicalSlot {
        self.0.slot()
    }

    pub(crate) fn into_id(self) -> identity::Identity<'d> {
        self.0
    }

    pub(crate) fn release(self) -> (identity::Identity<'d>, Solo) {
        let identity = self.0;
        let slot = Solo(identity.slot());
        (identity, slot)
    }
}

impl<'a> Outbound<'a> {
    pub(crate) fn try_map<B: route::Tag>(
        driver: driver::Reference<'a>,
        count: Capacity,
    ) -> Result<outbound::Reserved<'a, B>, outbound::Error> {
        Self::from(driver.descriptors().domain())
            .try_assign(route::OutboundKey::for_tag::<B>(), count)?;
        Ok(outbound::Reserved::new(driver))
    }
}

impl<'a> Outbound<'a> {
    pub(crate) fn slot(
        &self,
        owner: route::OutboundKey,
        local: indices::SlotIndex,
    ) -> Option<handles::OutboundSlot> {
        let table = &self.0.outbound;
        let lease = table.routes[owner.raw() as usize].get()?;
        let index = table.get(lease, local)?;
        Some(handles::OutboundSlot::new(index.0, owner))
    }

    pub(crate) fn creation(
        &self,
        owner: route::OutboundKey,
        local: indices::SlotIndex,
    ) -> Option<Creation> {
        self.slot(owner, local).map(Creation)
    }

    pub(crate) fn begin_create(&self, slot: handles::OutboundSlot) {
        state::get(self.0, slot.physical()).set(states::SlotState::route(
            states::Phase::CREATING_ONE,
            slot.owner(),
        ));
    }

    pub(crate) fn created(&self, creation: Creation) -> Create {
        let slot = creation.0;
        let owner = slot.owner();
        let physical = slot.physical();
        let state = state::get(self.0, physical);
        if state.get().phase() == states::Phase::CREATING_ONE {
            state.set(states::SlotState::route(states::Phase::OPEN_TWO, owner));
            Create::Deliver
        } else {
            state.set(states::SlotState::route(states::Phase::CLOSING_ZERO, owner));
            self.0.resident.retire().enqueue(physical);
            Create::Retired
        }
    }

    pub(crate) fn failed(&self, creation: Creation) {
        let slot = creation.0;
        let owner = slot.owner();
        let state = state::get(self.0, slot.physical());
        if state.get().phase() == states::Phase::CREATING_ONE {
            state.set(states::SlotState::route(states::Phase::FAILED_ONE, owner));
        } else {
            self.0.release_slot(slot);
        }
    }

    pub(crate) fn activate(&self, slot: handles::OutboundSlot) {
        state::get(self.0, slot.physical()).set(states::SlotState::route(
            states::Phase::OPEN_ONE,
            slot.owner(),
        ));
    }
}

impl Closures<'_> {
    pub(crate) fn direct(self, slot: handles::OutboundSlot) {
        state::get(self.0, slot.physical()).set(states::SlotState::route(
            states::Phase::CLOSING_ZERO,
            slot.owner(),
        ));
    }

    pub(crate) fn creating(self, id: identity::Identity<'_, handles::OutboundSlot>) -> Close {
        let slot = id.slot();
        let owner = slot.owner();
        let state = state::get(self.0, slot.physical());
        let phase = state.get().phase();
        if phase == states::Phase::CREATING_ONE {
            state.set(states::SlotState::route(
                states::Phase::CREATING_ZERO,
                owner,
            ));
            return Close::Settled;
        }
        Self::close(state, phase, owner)
    }

    pub(crate) fn created(self, slot: handles::OutboundSlot) -> Close {
        let owner = slot.owner();
        let state = state::get(self.0, slot.physical());
        let phase = state.get().phase();
        Self::close(state, phase, owner)
    }

    fn close(
        state: resident::Lifecycle<'_>,
        phase: states::Phase,
        owner: route::OutboundKey,
    ) -> Close {
        if phase == states::Phase::OPEN_TWO {
            state.set(states::SlotState::route(states::Phase::CLOSING_ONE, owner));
            return Close::Submit;
        }
        if phase == states::Phase::CLOSING_ONE {
            state.set(states::SlotState::route(states::Phase::CLOSING_ZERO, owner));
            return Close::Settled;
        }
        Close::Released
    }

    pub(crate) fn complete(self, slot: handles::PhysicalSlot) {
        let state = state::get(self.0, slot);
        let Some(owner) = state.get().owner() else {
            return;
        };
        let outbound = handles::OutboundSlot::new(SlotIndex::from(slot), owner);
        if state.get().phase() == states::Phase::CLOSING_ZERO {
            self.0.release_slot(outbound);
        } else {
            state.set(states::SlotState::route(states::Phase::CLOSED_ONE, owner));
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Creation>() == mem::size_of::<handles::OutboundSlot>());
    assert!(mem::align_of::<Creation>() == mem::align_of::<handles::OutboundSlot>());
    assert!(mem::size_of::<Closures<'_>>() == mem::size_of::<&Domain>());
    assert!(mem::size_of::<cell::Cell<Index>>() == mem::size_of::<cell::Cell<u32>>());
    assert!(mem::size_of::<Solo>() == mem::size_of::<handles::PhysicalSlot>());
    assert!(
        mem::size_of::<Solo<identity::Identity<'_>>>() == mem::size_of::<identity::Identity<'_>>()
    );
};

/// Access always selects the partition and resident state of the same Domain.
/// No prefix snapshot, lease, or unfinished transition can escape this module.
impl<'a> Outbound<'a> {
    /// Admits one mapping entry before extending the allocated prefix.
    pub(crate) fn try_reserve(self) -> Result<Solo<identity::Identity<'a>>, slot::Error> {
        let table = &self.0.outbound;
        let mut prefix = table.prefix.get();
        let Some(entry) = table.mapping.get(prefix.allocated as usize) else {
            return Err(if self.has_retiring() {
                slot::Error::Retiring { available: 0 }
            } else {
                slot::Error::Capacity { available: 0 }
            });
        };
        let index = entry.get();
        prefix.allocated += 1;
        table.prefix.set(prefix);
        let physical = handles::PhysicalSlot::from_index(index.0);
        let version = self.0.resident.claim(index, states::SlotState::STANDALONE);
        Ok(Solo(identity::Identity::new(physical, version)))
    }

    /// Consumes one still-counted admission from this driver's retained owner.
    pub(crate) fn release_one(&self, released: Solo) {
        let table = &self.0.outbound;
        let mut prefix = table.prefix.get();
        prefix.allocated -= 1;
        // SAFETY: the consumed admission belongs to this Domain's allocated
        // prefix, so its final position is inside the immutable mapping extent.
        let entry = unsafe { table.mapping.get_unchecked(prefix.allocated as usize) };
        entry.set(Index(released.0.into()));
        table.prefix.set(prefix);
        state::get(self.0, released.0).set(states::SlotState::FREE);
    }

    /// Admits a disjoint nonempty interval and both bounded prefix successors.
    fn try_assign(self, owner: route::OutboundKey, count: Capacity) -> Result<(), Error> {
        let Some(len) = count.span() else {
            return Err(Error::Empty);
        };
        let table = &self.0.outbound;
        let route = &table.routes[owner.raw() as usize];
        if route.get().is_some() {
            return Err(Error::Occupied);
        }
        let prefix = table.prefix.get();
        let avail = table.capacity.raw() - prefix.allocated;
        if len.get() > avail {
            return Err(if self.has_retiring() {
                Error::Retiring {
                    requested: len.get() as usize,
                    available: avail as usize,
                }
            } else {
                Error::Capacity {
                    requested: len.get() as usize,
                    available: avail as usize,
                }
            });
        }
        let lease = Lease {
            start: prefix.mapped,
            len,
            state: State::Live,
        };
        let next = Prefix {
            mapped: prefix.mapped + len.get(),
            allocated: prefix.allocated + len.get(),
        };
        table.rotate(
            prefix.mapped as usize,
            prefix.allocated as usize,
            next.allocated as usize,
        );
        for entry in table
            .mapping
            .iter()
            .skip(lease.start as usize)
            .take(len.get() as usize)
        {
            state::get_index(self.0, entry.get())
                .set(states::SlotState::route(states::Phase::MAPPED, owner));
        }
        route.set(Some(lease));
        table.prefix.set(next);
        Ok(())
    }

    pub(crate) fn acquire(
        &self,
        owner: route::OutboundKey,
        local: indices::SlotIndex,
    ) -> Option<identity::Identity<'a, handles::OutboundSlot>> {
        let table = &self.0.outbound;
        let lease = table.routes[owner.raw() as usize].get()?;
        if lease.state == State::Retired {
            return None;
        }
        let index = table.get(lease, local)?;
        let state = state::get_index(self.0, index);
        if state.is_exhausted() || !state.get().is_state(states::Phase::MAPPED, owner) {
            return None;
        }
        let version = self.0.resident.claim(
            index,
            states::SlotState::route(states::Phase::UNOPENED_ONE, owner),
        );
        Some(identity::Identity::outbound(
            handles::OutboundSlot::new(index.0, owner),
            version,
        ))
    }

    pub(crate) fn retire(&self, owner: route::OutboundKey) {
        let cell = &self.0.outbound.routes[owner.raw() as usize];
        if let Some(mut lease) = cell.get() {
            lease.state = State::Retired;
            cell.set(Some(lease));
        }
        self.reclaim(owner);
    }

    fn has_retiring(&self) -> bool {
        self.0.outbound.routes.iter().any(|route| {
            route
                .get()
                .is_some_and(|route| route.state == State::Retired)
        }) || self.0.resident.has_retiring()
    }

    /// Removes a retired interval; backward compaction preserves unread entries.
    /// Exhausted slots retain anonymous allocations; only reusable slots enter the free suffix.
    pub(super) fn reclaim(&self, owner: route::OutboundKey) {
        let table = &self.0.outbound;
        let cell = &table.routes[owner.raw() as usize];
        let Some(removed) = cell.get() else {
            return;
        };
        if removed.state == State::Live {
            return;
        }
        let slots = removed.range();
        if !table
            .mapping
            .iter()
            .skip(slots.start)
            .take(slots.len())
            .all(|entry| {
                state::get_index(self.0, entry.get())
                    .get()
                    .is_state(states::Phase::MAPPED, owner)
            })
        {
            return;
        }
        let prefix = table.prefix.get();
        let mut next = Prefix {
            mapped: prefix.mapped - removed.len.get(),
            allocated: prefix.allocated,
        };
        table.rotate(slots.start, slots.end, prefix.allocated as usize);
        let released = table
            .mapping
            .iter()
            .take(prefix.allocated as usize)
            .skip((prefix.allocated - removed.len.get()) as usize);
        let reusable = released.clone().rev().filter_map(|entry| {
            let index = entry.get();
            let state = state::get_index(self.0, index);
            if state.is_exhausted() {
                state.set(states::SlotState::STANDALONE);
                None
            } else {
                state.set(states::SlotState::FREE);
                Some(index)
            }
        });
        for (entry, index) in released.rev().zip(reusable) {
            entry.set(index);
            next.allocated -= 1;
        }
        table.reverse(next.allocated as usize..prefix.allocated as usize);
        for cell in &table.routes {
            if let Some(mut lease) = cell.get()
                && lease.start > removed.start
            {
                lease.start -= removed.len.get();
                cell.set(Some(lease));
            }
        }
        cell.set(None);
        table.prefix.set(next);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Lease>() == 3 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Lease>>() == std::mem::size_of::<Lease>());
};
