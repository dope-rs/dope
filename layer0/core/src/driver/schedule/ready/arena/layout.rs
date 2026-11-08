use std::num;

use crate::driver::{route::table, schedule::ready, settings};

pub(in crate::driver::schedule::ready) const TILE_SLOTS: usize = 2;

#[derive(Clone, Copy)]
#[repr(C)]
pub(in crate::driver::schedule::ready) struct Layout {
    pub(in crate::driver::schedule::ready) descriptors: table::Capacity,
    pub(in crate::driver::schedule::ready) capacity: settings::Ready,
}

impl Layout {
    pub(in crate::driver::schedule::ready) const fn new(plan: settings::Plan) -> Self {
        let config = plan.config();
        Self {
            descriptors: config.file_slots().capacity(),
            capacity: plan.ready(),
        }
    }

    pub(in crate::driver::schedule::ready) fn tile_count(self) -> usize {
        ((self.capacity.raw() - self.descriptors.raw()) as usize).div_ceil(TILE_SLOTS)
    }

    pub(in crate::driver::schedule::ready) fn available(
        self,
    ) -> Option<(ready::FreeIndex, num::NonZero<u32>)> {
        let count = num::NonZero::<u32>::new(self.capacity.raw() - self.descriptors.raw())?;
        Some((Self::free_index(self.descriptors.raw()), count))
    }

    pub(in crate::driver::schedule::ready) fn next_free(self, index: u32) -> ready::FreeIndex {
        let next = index + 1;
        if next == self.capacity.raw() {
            Self::free_index(index)
        } else {
            Self::free_index(next)
        }
    }

    fn free_index(index: u32) -> ready::FreeIndex {
        // SAFETY: callers supply an index below the admitted capacity, at most u32::MAX.
        let code = unsafe { num::NonZero::<u32>::new_unchecked(index + 1) };
        ready::FreeIndex::from_code(code)
    }
}
