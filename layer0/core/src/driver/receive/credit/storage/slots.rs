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
    pub(super) active: storage::Active<'owner, T, S, C>,
}

impl<'owner, T: Copy, S: Copy, C> Slots<'owner, T, S, C> {
    pub(super) fn new(active: storage::Active<'owner, T, S, C>) -> Self {
        Self { active }
    }

    pub fn get(
        &self,
        index: indices::SlotIndex,
    ) -> Result<storage::Slot<'_, 'owner, T, S, C>, error::Index> {
        storage::Slot::new(&self.active, index)
    }

    pub fn iter(&self) -> impl Iterator<Item = storage::Slot<'_, 'owner, T, S, C>> {
        storage::Slot::iter(&self.active)
    }
}
