use crate::driver::route::{self, table};

pub struct Entry<'a, T, Tag: route::Tag>(table::ExternalOccupiedEntry<'a, T, Tag>);

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub(in crate::driver::route::table) fn new(
        inner: table::ExternalOccupiedEntry<'a, T, Tag>,
    ) -> Self {
        Self(inner)
    }

    pub fn key(&self) -> table::Key<Tag> {
        let key = self.0.key();
        table::Key::new(
            route::SlotIndex::from_bounded(key.index()),
            key.generation(),
        )
    }

    pub fn get(&self) -> &T {
        self.0.get()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub fn remove(self) -> T {
        self.0.remove()
    }
}
