use o3::depot::arena::slab::external::OccupiedSlot;

use crate::driver::{
    route,
    route::table::{self, PARTITIONS},
};

#[repr(transparent)]
pub struct Entry<'a, T, Tag: route::Tag>(
    pub(in crate::driver::route::table) OccupiedSlot<'a, T, route::Epoch, Tag, PARTITIONS>,
);

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub fn key(&self) -> table::Key<Tag> {
        table::Key::new(self.0.key())
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

    /// Destroys the occupied value in place before returning its exact slot.
    pub fn discard(self) {
        self.0.discard();
    }

    pub fn try_remove<R, E>(
        self,
        take: impl FnOnce(T) -> Result<R, (T, E)>,
    ) -> Result<R, (Self, E)> {
        self.0
            .try_remove(take)
            .map_err(|(entry, error)| (Self(entry), error))
    }
}
