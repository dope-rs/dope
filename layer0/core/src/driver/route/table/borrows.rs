use external::errors;
use o3::depot::{
    arena::slab::{self, external},
    capacity,
};
use table::entries::{occupied, vacant};

use crate::driver::{
    route,
    route::{indices, table, table::conn},
};

#[repr(transparent)]
pub struct View<'a, T, Tag: route::Tag> {
    pub(in crate::driver::route::table) inner: &'a table::Storage<T, Tag>,
}

impl<'a, T, Tag: route::Tag> View<'a, T, Tag> {
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

    pub fn get(self, parts: table::Parts<Tag>) -> Option<&'a T> {
        self.inner.get(parts.external())
    }

    pub fn contains(self, parts: table::Parts<Tag>) -> bool {
        self.inner.get(parts.external()).is_some()
    }

    pub fn key_at(self, slot: indices::SlotIndex) -> Option<table::Key<Tag>> {
        self.inner.key_at(slot.raw()).map(table::Key::new)
    }
}

#[repr(transparent)]
pub struct Access<'a, T, Tag: route::Tag>(
    pub(in crate::driver::route::table) &'a mut table::Storage<T, Tag>,
);

impl<'a, T, Tag: route::Tag> Access<'a, T, Tag> {
    pub fn advance_to(self, floor: Option<route::Epoch>) {
        self.0.advance_to(floor);
    }

    pub fn get(self, parts: table::Parts<Tag>) -> Option<&'a mut T> {
        self.0.get_mut(parts.external())
    }

    pub fn occupied(self, parts: table::Parts<Tag>) -> Option<occupied::Entry<'a, T, Tag>> {
        self.0.occupied(parts.external()).map(occupied::Entry)
    }

    pub fn try_occupied(
        self,
        parts: table::Parts<Tag>,
    ) -> Result<occupied::Entry<'a, T, Tag>, errors::Lookup<route::Epoch, Tag>> {
        let entry = self.0.try_occupied(parts.external())?;
        Ok(occupied::Entry(entry))
    }

    /// Borrows the current owner directly from this table's physical slot.
    pub fn at(self, index: indices::SlotIndex) -> Option<occupied::Entry<'a, T, Tag>> {
        self.0.occupied_at(index.raw()).map(occupied::Entry)
    }
}

#[repr(transparent)]
pub struct Grant<'a, T, Tag: route::Tag>(
    external::Grant<'a, T, route::Epoch, Tag, { u32::MAX }, { table::PARTITIONS }>,
);

impl<'a, T, Tag: route::Tag> Grant<'a, T, Tag> {
    pub fn reserve(self) -> vacant::Entry<'a, T, Tag> {
        vacant::Entry {
            inner: self.0.reserve(),
        }
    }
}

#[repr(transparent)]
pub struct Vacancy<'a, T, Tag: route::Tag>(
    pub(in crate::driver::route::table) &'a mut table::Storage<T, Tag>,
);

impl<'a, T, Tag: route::Tag> Vacancy<'a, T, Tag> {
    pub fn try_reserve(self) -> Result<vacant::Entry<'a, T, Tag>, slab::Vacancy> {
        let grant = self.0.try_grant()?;
        Ok(vacant::Entry {
            inner: grant.reserve(),
        })
    }

    pub fn try_head(self, capacity: conn::Capacity) -> Result<Grant<'a, T, Tag>, slab::Vacancy> {
        self.0.try_grant_in(0..capacity.count().get()).map(Grant)
    }

    /// Selects physical capacity before identity, excluding one held slot.
    pub fn try_except(
        self,
        capacity: conn::Capacity,
        excluded: indices::SlotIndex,
    ) -> Result<Grant<'a, T, Tag>, slab::Vacancy> {
        self.0
            .try_except(0..capacity.count().get(), excluded.raw())
            .map(Grant)
    }
}
