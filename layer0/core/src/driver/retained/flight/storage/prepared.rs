use std::ptr;

use crate::driver::retained::flight::storage::{self, Preparation, Slots, state};

/// Fully staged flight acquisition retaining its exact arena and preparation loan.
#[must_use = "prepared flight groups must be committed"]
pub struct Prepared<
    'a,
    'owner,
    T: Copy,
    const INDEX_BITS: u32 = 32,
    const GENERATION_BITS: u32 = 32,
    const GROUPS: usize = 1,
> {
    pub(super) access: storage::raw::Access<'a, 'owner, T, INDEX_BITS, GENERATION_BITS>,
    pub(super) states: [Preparation<'owner, T, INDEX_BITS, GENERATION_BITS>; GROUPS],
    pub(super) directory: Option<Vec<ptr::NonNull<state::State<T, INDEX_BITS, GENERATION_BITS>>>>,
}

impl<'owner, T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32, const GROUPS: usize>
    Prepared<'_, 'owner, T, INDEX_BITS, GENERATION_BITS, GROUPS>
{
    /// Publishes into the arena used to prepare this batch, without allocation.
    pub fn groups(self) -> [Slots<'owner, T, INDEX_BITS, GENERATION_BITS>; GROUPS] {
        let Self {
            access,
            states,
            directory: next,
        } = self;
        access.commit(states, next)
    }
}
