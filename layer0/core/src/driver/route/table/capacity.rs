use std::num;

use o3::{base::num::bounded, depot::capacity};

use crate::driver::{
    route,
    route::{
        indices,
        table::{
            Iter, bounds,
            bounds::{Outside, Slot},
        },
    },
};

/// Maximum live slot count in one route table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Capacity(pub(in crate::driver) capacity::Count);

impl Capacity {
    /// Admits a nonempty table count while retaining its lower bound.
    pub fn try_nonzero(
        count: num::NonZero<u32>,
    ) -> Result<bounded::NonZeroU32<1, { route::SLOT_MASK as u32 + 1 }>, super::Invalid> {
        Self::try_from(count.get() as usize)?;
        // SAFETY: the input retains nonzero and admission proved the unchanged upper bound.
        Ok(unsafe { bounded::raw::NonZeroProof::assume(count) }.retain())
    }

    pub const fn count(self) -> capacity::Count {
        self.0
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0.raw()
    }

    pub(crate) fn span(self) -> Option<num::NonZero<u32>> {
        num::NonZero::<u32>::new(self.raw())
    }

    pub const fn contains(self, index: indices::SlotIndex) -> bool {
        index.raw() < self.raw()
    }

    /// The following slot when it remains inside this table.
    pub const fn next(self, index: indices::SlotIndex) -> Option<indices::SlotIndex> {
        let raw = index.raw() + 1;
        if raw < self.raw() {
            Some(indices::SlotIndex::from_field(raw as u64))
        } else {
            None
        }
    }

    pub const fn excludes(self, index: indices::SlotIndex) -> Option<bounds::Excluded> {
        if self.contains(index) {
            None
        } else {
            Some(bounds::Excluded::new(index, self))
        }
    }

    pub fn slot(self, index: usize) -> Result<indices::SlotIndex, Slot> {
        let slot = indices::SlotIndex::try_from(index).map_err(|outside| {
            Slot::Abi(Outside::new(
                outside.actual(),
                route::SLOT_MASK as usize + 1,
            ))
        })?;
        if self.contains(slot) {
            Ok(slot)
        } else {
            Err(Slot::Capacity(Outside::new(index, self.raw() as usize)))
        }
    }
}

impl From<bounded::NonZeroU32<1, { route::SLOT_MASK as u32 + 1 }>> for Capacity {
    fn from(count: bounded::NonZeroU32<1, { route::SLOT_MASK as u32 + 1 }>) -> Self {
        Self(capacity::Count::new(count.get()))
    }
}

impl TryFrom<usize> for Capacity {
    type Error = super::Invalid;

    fn try_from(actual: usize) -> Result<Self, Self::Error> {
        let maximum = route::SLOT_MASK as usize + 1;
        if actual > maximum {
            return Err(super::Invalid::from_bounds(actual, 0, maximum));
        }
        Ok(Self(capacity::Count::new(actual as u32)))
    }
}

impl IntoIterator for Capacity {
    type Item = indices::SlotIndex;
    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        Iter(0..self.raw())
    }
}
