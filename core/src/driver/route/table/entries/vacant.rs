use crate::driver::route::{
    self,
    table::{self, entries::occupied},
};

pub struct Entry<'a, T, Tag: route::Tag> {
    inner: table::ExternalVacantEntry<'a, T, Tag>,
}

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub(in crate::driver::route::table) fn new(
        inner: table::ExternalVacantEntry<'a, T, Tag>,
    ) -> Self {
        Self { inner }
    }

    pub fn insert(self, value: T) {
        let _ = self.inner.insert(value);
    }

    pub fn insert_occupied(self, value: T) -> occupied::Entry<'a, T, Tag> {
        occupied::Entry::new(self.inner.insert_occupied(value))
    }

    pub fn key(&self) -> table::Key<Tag> {
        let key = self.inner.key();
        table::Key::new(
            route::SlotIndex::from_bounded(key.index()),
            key.generation(),
        )
    }
}
