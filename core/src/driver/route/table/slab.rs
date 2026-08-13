use std::{ops, process};

use o3::collections::{self, slab};
use table::entries::{occupied, vacant};

use crate::driver::route::{self, table};

pub struct Slab<T, Tag: route::Tag> {
    inner: table::ExternalSlab<T, Tag>,
}

impl<T, Tag: route::Tag> Slab<T, Tag> {
    pub fn with_capacity(capacity: table::Capacity) -> Self {
        match Self::try_with_capacity(capacity) {
            Ok(table) => table,
            Err(_) => process::abort(),
        }
    }

    /// Reserves the complete typed target table transactionally.
    pub fn try_with_capacity(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            inner: table::ExternalSlab::try_with_capacity(capacity.0)?,
        })
    }

    pub fn capacity(&self) -> table::Capacity {
        table::Capacity(slab::Capacity::new(self.inner.capacity() as u32))
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn insert(&mut self, value: T) -> Result<table::Key<Tag>, T> {
        self.inner.insert(value).map(table::Key::from_external)
    }

    pub fn vacant_entry(&mut self) -> Option<vacant::Entry<'_, T, Tag>> {
        Some(vacant::Entry::new(self.inner.vacant_entry()?))
    }

    pub fn vacant_entry_at(&mut self, index: u32) -> Option<vacant::Entry<'_, T, Tag>> {
        Some(vacant::Entry::new(self.inner.vacant_entry_at(index)?))
    }

    pub fn vacant_entry_in(&mut self, range: ops::Range<u32>) -> Option<vacant::Entry<'_, T, Tag>> {
        Some(vacant::Entry::new(self.inner.vacant_entry_in(range)?))
    }

    pub fn get(&self, key: table::Key<Tag>) -> Option<&T> {
        self.inner.get(key.external())
    }

    pub fn occupied_entry(&mut self, key: table::Key<Tag>) -> Option<occupied::Entry<'_, T, Tag>> {
        Some(occupied::Entry::new(
            self.inner.occupied_entry(key.external())?,
        ))
    }

    pub fn occupied_entry_parts(
        &mut self,
        parts: table::Parts<Tag>,
    ) -> Option<occupied::Entry<'_, T, Tag>> {
        Some(occupied::Entry::new(
            self.inner
                .occupied_entry(table::Key::external_parts(parts))?,
        ))
    }

    pub fn entries(&self) -> table::Entries<'_, T, Tag> {
        table::Entries::new(self.inner.entries())
    }

    pub fn entries_mut(&mut self) -> table::Mut<'_, T, Tag> {
        table::Mut::new(self.inner.entries_mut())
    }

    pub fn remove(&mut self, key: table::Key<Tag>) -> Option<T> {
        self.inner.remove(key.external())
    }

    pub fn remove_parts(&mut self, parts: table::Parts<Tag>) -> Option<T> {
        self.inner.remove(table::Key::external_parts(parts))
    }
}
