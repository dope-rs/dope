use std::num;

use crate::driver::{route::table, schedule::ready, settings};

pub(in crate::driver::schedule::ready) const TILE_SLOTS: usize = 2;

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct Layout {
    pub(in crate::driver::schedule::ready) descriptors: table::Capacity,
    dynamic: u16,
    pub(in crate::driver::schedule::ready) capacity: settings::Ready,
}

impl Layout {
    pub(in crate::driver::schedule::ready) const fn new(plan: settings::Plan) -> Self {
        let config = plan.config();
        Self {
            descriptors: config.file_slots().capacity(),
            dynamic: plan.ready_slots(),
            capacity: plan.ready(),
        }
    }

    pub(in crate::driver::schedule::ready) fn tile_count(self) -> usize {
        usize::from(self.dynamic).div_ceil(TILE_SLOTS)
    }

    pub(in crate::driver::schedule::ready) fn available(
        self,
    ) -> Option<(ready::FreeIndex, num::NonZero<u16>)> {
        let count = num::NonZero::<u16>::new(self.dynamic)?;
        Some((Self::free_index(self.descriptors.raw()), count))
    }

    pub(in crate::driver::schedule::ready) fn next_free(self, index: u32) -> ready::FreeIndex {
        let next = index + 1;
        if next == u32::from(self.capacity.raw()) {
            Self::free_index(index)
        } else {
            Self::free_index(next)
        }
    }

    fn free_index(index: u32) -> ready::FreeIndex {
        // SAFETY: Plan bounds the complete descriptor + dynamic arena below
        // u16::MAX, and callers supply an index inside that arena.
        let code = unsafe { num::NonZero::<u16>::new_unchecked(index as u16 + 1) };
        ready::FreeIndex::from_code(code)
    }
}
