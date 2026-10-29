use std::{cell, mem, ops};

use dope::core::driver::route::KeyTag;
use o3::alloc;

use crate::{
    core::driver::route::{
        indices,
        table::{self, conn},
    },
    manifold::{app::dispatch::identity, client::conn::Id},
    net::port::entry,
};

/// Stable entry storage owning its exact connection domain and shutdown cursor.
pub(in crate::net::port::table) struct Entries<'d, E, I: identity::Identity> {
    items: Box<[entry::Entry<'d, E, I>]>,
    capacity: conn::Capacity,
    shutdown: cell::Cell<Option<indices::SlotIndex>>,
}

/// One shutdown entry borrowed from the owner that selected it.
pub(in crate::net::port::table) struct Shutdown<'a, 'd, E, I: identity::Identity> {
    entries: &'a Entries<'d, E, I>,
    slot: indices::SlotIndex,
    entry: &'a entry::Entry<'d, E, I>,
}

impl<'d, E, I: identity::Identity> Entries<'d, E, I> {
    pub(in crate::net::port::table) fn try_new(
        capacity: conn::Capacity,
    ) -> Result<Self, alloc::Error> {
        let items = alloc::BoxSliceExt::try_default(capacity.count().get() as usize)?;
        Ok(Self {
            items,
            capacity,
            shutdown: cell::Cell::new(None),
        })
    }

    pub(in crate::net::port::table) const fn capacity(&self) -> conn::Capacity {
        self.capacity
    }

    pub(in crate::net::port::table) fn get(
        &self,
        slot: indices::SlotIndex,
    ) -> Option<&entry::Entry<'d, E, I>> {
        self.items.get(slot.index().get() as usize)
    }

    pub(in crate::net::port::table) fn close(&self) {
        self.shutdown.set(Some(indices::SlotIndex::ZERO));
    }

    pub(in crate::net::port::table) fn is_closing(&self) -> bool {
        self.shutdown.get().is_some()
    }

    pub(in crate::net::port::table) fn shutdown(&self) -> Option<Shutdown<'_, 'd, E, I>> {
        let slot = self.shutdown.get()?;
        // SAFETY: construction gives this owner exactly capacity entries and
        // capacity is nonzero. Its cursor starts at ZERO; only Shutdown::advance
        // changes it, using this same immutable capacity. No external index can
        // enter this cursor or replace the retained entry slice.
        let entry = unsafe { self.items.get_unchecked(slot.index().get() as usize) };
        Some(Shutdown {
            entries: self,
            slot,
            entry,
        })
    }

    /// Returns an entry for a slot already proven live by a queue link.
    ///
    /// # Safety
    /// `slot` must be one of this table's live queue identities.
    pub(in crate::net::port::table) unsafe fn bind(
        &self,
        slot: indices::SlotIndex,
    ) -> &entry::Entry<'d, E, I> {
        unsafe { self.items.get_unchecked(slot.index().get() as usize) }
    }
}

impl<E, I: identity::Identity> Shutdown<'_, '_, E, I> {
    pub(in crate::net::port::table) const fn index(&self) -> indices::SlotIndex {
        self.slot
    }

    pub(in crate::net::port::table) fn advance(self) {
        let slots = table::Capacity::from(self.entries.capacity);
        self.entries.shutdown.set(slots.next(self.slot));
    }
}

impl<'d, E, I: identity::Identity> ops::Deref for Shutdown<'_, 'd, E, I> {
    type Target = entry::Entry<'d, E, I>;

    fn deref(&self) -> &Self::Target {
        self.entry
    }
}

const _: () = {
    assert!(
        mem::size_of::<Entries<'static, (), Id<'static, KeyTag<0>>>>()
            == mem::size_of::<(
                Box<[entry::Entry<'static, (), Id<'static, KeyTag<0>>>]>,
                conn::Capacity,
                Option<indices::SlotIndex>,
            )>()
    );
    assert!(
        mem::align_of::<Entries<'static, (), Id<'static, KeyTag<0>>>>()
            == mem::align_of::<Box<[entry::Entry<'static, (), Id<'static, KeyTag<0>>>]>>()
    );
};
