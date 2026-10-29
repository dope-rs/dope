use std::{cell, num::NonZero, ops};

use o3::{
    alloc,
    alloc::VecExt as _,
    base::{marker::ThreadBound, num},
};
use outbound::Index;
pub(crate) use outbound::{Close, Closures, Create, Creation, Outbound, Solo};

use crate::{
    driver::{
        route,
        route::{indices, indices::SlotIndex, table, table::Capacity},
        settings,
    },
    io::fd::{handles, identity, states},
};

mod outbound;

mod resident;
pub(crate) mod retire;
mod state;

pub(crate) struct Domain {
    resident: resident::Store,
    accept: table::Capacity,
    closed: cell::Cell<retire::Completed>,
    outbound: Table,
}

impl TryFrom<settings::FileSlots> for Domain {
    type Error = alloc::Error;

    fn try_from(layout: settings::FileSlots) -> Result<Self, Self::Error> {
        Ok(Self {
            resident: resident::Store::try_new(layout.capacity())?,
            accept: layout.accept(),
            closed: cell::Cell::new(retire::Completed::EMPTY),
            outbound: Table::try_new(layout)?,
        })
    }
}

impl Domain {
    pub(crate) const fn retire(&self) -> retire::View<'_> {
        retire::View {
            domain: self,
            thread: ThreadBound::NEW,
        }
    }

    pub(crate) fn claim<'d>(
        &'d self,
        slot: handles::PhysicalSlot,
    ) -> Option<identity::Identity<'d>> {
        let version = self.resident.try_claim(SlotIndex::from(slot))?;
        Some(identity::Identity::new(slot, version))
    }

    pub(crate) fn release(&self, id: identity::Identity<'_>) {
        self.resident.retire().release(id).cancel();
    }

    pub(crate) fn set_target(&self, id: identity::Identity<'_>, target: route::Token) {
        self.resident
            .set_target(id.index(), id.generation(), target);
    }

    pub(crate) fn target(&self, id: identity::Identity<'_>) -> Option<route::Token> {
        self.resident.target(id.index(), id.generation())
    }

    pub(crate) fn target_at(&self, index: indices::SlotIndex) -> Option<route::Token> {
        self.resident.target_at(index)
    }

    pub(crate) fn target_for(
        &self,
        index: indices::SlotIndex,
        version: num::Generation,
    ) -> Option<route::Token> {
        self.resident.target(index, version)
    }

    pub(crate) const fn accept_cap(&self) -> table::Capacity {
        self.accept
    }

    pub(crate) const fn outbound_cap(&self) -> table::Capacity {
        self.outbound.capacity()
    }

    fn release_slot(&self, slot: handles::OutboundSlot) {
        let owner = slot.owner();
        state::get(self, slot.physical())
            .set(states::SlotState::route(states::Phase::MAPPED, owner));
        Outbound::from(self).reclaim(owner);
    }
}

/// The allocated prefix of one immutable mapping extent, mutated only here.
/// Every transition preserves mapped <= allocated <= mapping.len().
/// The free suffix contains reusable slots; exhausted slots stay allocated.
#[derive(Clone, Copy)]
struct Prefix {
    mapped: u32,
    allocated: u32,
}

#[derive(Clone, Copy)]
struct Lease {
    start: u32,
    len: NonZero<u32>,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Live,
    Retired,
}

pub(crate) struct Table {
    capacity: Capacity,
    mapping: Box<[cell::Cell<Index>]>,
    prefix: cell::Cell<Prefix>,
    routes: [cell::Cell<Option<Lease>>; route::CAPACITY],
}

impl Lease {
    fn range(self) -> ops::Range<usize> {
        self.start as usize..(self.start + self.len.get()) as usize
    }

    fn at(self, local: indices::SlotIndex) -> Option<usize> {
        (local.raw() < self.len.get()).then_some((self.start + local.raw()) as usize)
    }
}

impl Table {
    fn try_new(layout: settings::FileSlots) -> Result<Self, alloc::Error> {
        let capacity = layout.outbound();
        let mut mapping = Vec::try_new(capacity.raw() as usize)?;
        mapping.extend(layout.slots().map(|slot| cell::Cell::new(Index(slot))));
        Ok(Self {
            capacity,
            mapping: mapping.into_boxed_slice(),
            prefix: cell::Cell::new(Prefix {
                mapped: 0,
                allocated: 0,
            }),
            routes: [const { cell::Cell::new(None) }; route::CAPACITY],
        })
    }

    const fn capacity(&self) -> Capacity {
        self.capacity
    }

    /// Looks up a local slot in the exact lease the caller read from this table.
    fn get(&self, lease: Lease, local: indices::SlotIndex) -> Option<Index> {
        let physical = lease.at(local)?;
        // SAFETY: both callers read this table's own successfully admitted lease;
        // at bounds the local slot within its immutable, nonoverlapping extent.
        Some(unsafe { self.mapping.get_unchecked(physical) }.get())
    }

    fn rotate(&self, start: usize, middle: usize, end: usize) {
        self.reverse(start..middle);
        self.reverse(middle..end);
        self.reverse(start..end);
    }

    fn reverse(&self, range: ops::Range<usize>) {
        let swaps = range.len() / 2;
        let left = self.mapping.iter().skip(range.start).take(swaps);
        let right = self
            .mapping
            .iter()
            .take(range.end)
            .skip(range.start)
            .rev()
            .take(swaps);
        for (left, right) in left.zip(right) {
            left.swap(right);
        }
    }
}
