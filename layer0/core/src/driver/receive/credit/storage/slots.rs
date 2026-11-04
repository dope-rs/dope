use std::{marker, num};

use crate::driver::{
    receive::credit::{storage, storage::error},
    route::indices,
};

/// Exact active storage with exclusive retirement authority.
///
/// A checked slot borrows the active group through its final use.
/// Retirement consumes the active authority exactly once.
#[must_use = "dropping slots retires their group"]
pub struct Slots<'owner, T: Copy, S: Copy, C> {
    pub(super) group: &'owner storage::Group<C>,
    pub(super) entries: &'owner [storage::Entry<T, S, C>],
    pub(super) capacity: num::NonZero<u32>,
}

impl<'owner, T: Copy, S: Copy, C> Slots<'owner, T, S, C> {
    pub fn get(
        &self,
        index: indices::SlotIndex,
    ) -> Result<storage::Slot<'_, 'owner, T, S, C>, error::Index> {
        Ok(storage::Slot {
            entry: self.entries.get(index.raw() as usize).ok_or(error::Index {
                requested: index,
                capacity: self.capacity,
            })?,
            active: marker::PhantomData,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = storage::Slot<'_, 'owner, T, S, C>> {
        self.entries.iter().map(|entry| storage::Slot {
            entry,
            active: marker::PhantomData,
        })
    }
}

impl<T: Copy, S: Copy, C> Drop for Slots<'_, T, S, C> {
    fn drop(&mut self) {
        let owners = self.iter().filter(|slot| slot.is_held()).count();
        self.group.attachment.set(storage::Attachment::Retired);
        self.group.retire(owners);
    }
}
