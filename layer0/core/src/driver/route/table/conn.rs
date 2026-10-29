use std::num;

use o3::{base::num::bounded, depot, depot::capacity};

use crate::driver::{route, route::table};

/// A non-empty target-table capacity with one encodable slot left as a sentinel.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(transparent)]
pub struct Capacity(bounded::NonZeroU32<1, { route::SLOT_MASK as u32 }>);

impl Capacity {
    pub const fn fixed<const COUNT: usize>() -> Self {
        const { assert!(COUNT > 0 && COUNT <= route::SLOT_MASK as usize, "capacity") };
        // SAFETY: the const bound keeps the count in 1..=SLOT_MASK.
        Self(
            unsafe {
                bounded::raw::NonZeroProof::assume(
                    num::NonZero::<u32>::MIN.saturating_add(COUNT as u32 - 1),
                )
            }
            .retain(),
        )
    }

    pub const fn nonzero(self) -> num::NonZero<usize> {
        num::NonZero::<usize>::MIN.saturating_add(self.raw() as usize - 1)
    }

    pub(crate) const fn raw(self) -> u32 {
        self.0.get()
    }

    pub const fn count(self) -> num::NonZero<u32> {
        self.0.nonzero()
    }

    pub const fn entries(self) -> capacity::Count {
        capacity::Count::new(self.0.get())
    }

    /// Scales this validated connection domain into a collection count.
    ///
    /// The factor is part of the type-level call site, so the complete
    /// connection domain must fit before any value can be scaled.
    pub const fn scale<const FACTOR: u32>(self) -> capacity::Count {
        const {
            assert!(FACTOR != 0);
            assert!(route::SLOT_MASK <= (u32::MAX / FACTOR) as u64);
        }
        capacity::Count::new(self.0.get() * FACTOR)
    }
}

impl From<num::NonZero<u16>> for Capacity {
    fn from(count: num::NonZero<u16>) -> Self {
        const { assert!(u16::MAX as u64 <= route::SLOT_MASK, "capacity bound") };
        // SAFETY: the const bound keeps every nonzero u16 count within 1..=SLOT_MASK.
        Self(unsafe { bounded::raw::NonZeroProof::assume(count.into()) }.retain())
    }
}

impl TryFrom<usize> for Capacity {
    type Error = table::Invalid;

    fn try_from(actual: usize) -> Result<Self, Self::Error> {
        bounded::NonZeroU32::from_usize(actual)
            .map(Self)
            .ok_or_else(|| table::Invalid::from_bounds(actual, 1, route::SLOT_MASK as usize))
    }
}

impl TryFrom<capacity::Count> for Capacity {
    type Error = table::Invalid;

    fn try_from(actual: capacity::Count) -> Result<Self, Self::Error> {
        bounded::NonZeroU32::new(actual.raw())
            .map(Self)
            .ok_or_else(|| table::Invalid::from_bounds(actual.get(), 1, route::SLOT_MASK as usize))
    }
}

impl From<Capacity> for table::Capacity {
    fn from(capacity: Capacity) -> Self {
        Self(capacity::Count::new(capacity.0.get()))
    }
}

// SAFETY: the retained nonzero connection domain is bounded by SLOT_MASK, which is one immutable u32 extent.
unsafe impl depot::raw::Count for Capacity {
    const MAX: u32 = route::SLOT_MASK as u32;

    fn count(self) -> capacity::Count {
        capacity::Count::new(self.0.get())
    }
}
