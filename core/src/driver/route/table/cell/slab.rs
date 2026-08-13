use std::process;

use o3::collections::{
    self,
    slab::{self, external},
};

use crate::driver::route::{self, table};

pub struct CellSlab<T, Tag: route::Tag> {
    inner: external::Cell<T, route::Epoch, Tag>,
}

impl<T, Tag: route::Tag> CellSlab<T, Tag> {
    pub fn with_capacity(capacity: table::Capacity) -> Self {
        match Self::try_with_capacity(capacity) {
            Ok(table) => table,
            Err(_) => process::abort(),
        }
    }

    /// Reserves the complete interior-mutable target table transactionally.
    pub fn try_with_capacity(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            inner: external::Cell::try_with_capacity(capacity.0)?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn capacity(&self) -> table::Capacity {
        table::Capacity(slab::Capacity::new(self.inner.capacity() as u32))
    }

    pub fn key_at(&self, position: usize) -> Option<table::Key<Tag>> {
        self.inner.key_at(position).map(table::Key::from_external)
    }

    pub fn keys(&self) -> impl Iterator<Item = table::Key<Tag>> + '_ {
        self.inner.keys().map(table::Key::from_external)
    }

    pub fn any_or_busy(&self, predicate: impl FnMut(&T) -> bool) -> bool {
        self.inner.any_or_busy(predicate)
    }

    pub fn insert(&self, value: T) -> Result<table::Key<Tag>, T> {
        self.inner.insert(value).map(table::Key::from_external)
    }

    pub fn try_insert_with<R, E>(
        &self,
        value: T,
        f: impl FnOnce(table::Key<Tag>, &mut T) -> Result<R, E>,
    ) -> Result<(table::Key<Tag>, R), slab::InsertError<T, E>> {
        self.inner
            .try_insert_with(value, |key, value| f(table::Key::from_external(key), value))
            .map(|(key, result)| (table::Key::from_external(key), result))
    }

    pub fn try_insert_build<I, R, E>(
        &self,
        input: I,
        build: impl FnOnce(I) -> T,
        f: impl FnOnce(table::Key<Tag>, &mut T) -> Result<R, E>,
    ) -> Result<(table::Key<Tag>, R), slab::BuildError<I, T, E>> {
        self.inner
            .try_insert_build(input, build, |key, value| {
                f(table::Key::from_external(key), value)
            })
            .map(|(key, result)| (table::Key::from_external(key), result))
    }

    pub fn update<R>(&self, key: table::Key<Tag>, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        self.inner.update(key.external(), f)
    }

    pub fn update_parts<R>(
        &self,
        parts: table::Parts<Tag>,
        f: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        self.inner.update(table::Key::external_parts(parts), f)
    }

    pub fn remove(&self, key: table::Key<Tag>) -> Option<T> {
        self.inner.remove(key.external())
    }

    pub fn remove_parts(&self, parts: table::Parts<Tag>) -> Option<T> {
        self.inner.remove(table::Key::external_parts(parts))
    }

    pub fn remove_parts_with<R>(
        &self,
        parts: table::Parts<Tag>,
        f: impl FnOnce(&mut T) -> Option<R>,
    ) -> Option<(T, R)> {
        self.inner.remove_with(table::Key::external_parts(parts), f)
    }
}
