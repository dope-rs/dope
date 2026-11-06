use o3::{alloc, depot::capacity};
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

    pub fn key_at(&self, slot: indices::SlotIndex) -> Option<table::Key<Tag>> {
        self.inner.key_at(slot.raw()).map(table::Key::new)
    }

    pub fn access(&mut self) -> borrows::Access<'_, T, Tag> {
        borrows::Access(&mut self.inner)
    }
}
