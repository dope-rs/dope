use o3::{
    alloc,
    depot::{arena::slab::external::OccupiedSlot, capacity, index::Initialized, raw::Proof},
};
use table::borrows;

use crate::driver::{route, route::indices, route::table};

#[repr(transparent)]
pub struct Slab<T, Tag: route::Tag> {
    inner: table::Storage<T, Tag>,
}

impl<T, Tag: route::Tag> Slab<T, Tag> {
    /// Reserves the complete typed target table transactionally.
    pub fn try_new(cap: table::Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: table::Storage::try_new(cap.0)?,
        })
    }

    /// Returns the immutable extent admitted when this route table was allocated.
    pub fn capacity(&self) -> table::Capacity {
        table::Capacity(self.inner.capacity())
    }

    pub fn len(&self) -> capacity::Count {
        self.inner.len()
    }

    /// Returns capacity minus the number of initialized values.
    ///
    /// Forgotten reservations are included, so this is an upper bound on
    /// reservable slots. Reservation still reports `Full` or `IdentityExhausted`.
    pub fn available(&self) -> capacity::Count {
        let total = self.inner.capacity();
        let occupied = self.inner.len();
        capacity::Count::new(total.raw() - occupied.raw())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn next_generation(&self) -> Option<route::Epoch> {
        self.inner.next_generation()
    }

    pub fn get(&self, parts: table::Parts<Tag>) -> Option<&T> {
        self.inner.get(parts.external())
    }

    pub fn contains(&self, parts: table::Parts<Tag>) -> bool {
        self.get(parts).is_some()
    }

    pub fn value(live: Proof<(&Self, indices::SlotIndex), Initialized<()>>) -> &T {
        // SAFETY: the proof retains this original table's initialized physical slot.
        let live = unsafe { live.map(|(owner, index)| (&owner.inner, index.index().into())) };
        table::Storage::value(live).into_inner().0
    }

    pub fn occupied<C>(
        live: Proof<((&mut Self, C), indices::SlotIndex), Initialized<()>>,
    ) -> (table::entries::occupied::Entry<'_, T, Tag>, C) {
        // SAFETY: the original exclusive table, initialized slot and context stay paired.
        let live = unsafe {
            live.map(|((owner, context), index)| {
                ((&mut owner.inner, context), index.index().into())
            })
        };
        let (entry, context) = OccupiedSlot::from_live(live);
        (table::entries::occupied::Entry(entry), context)
    }

    pub fn key_at(&self, slot: indices::SlotIndex) -> Option<table::Key<Tag>> {
        self.inner.key_at(slot.raw()).map(table::Key::new)
    }

    pub fn access(&mut self) -> borrows::Access<'_, T, Tag> {
        borrows::Access(&mut self.inner)
    }
}
