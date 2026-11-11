use std::num::NonZero;

use dope_sys::memory;
use o3::{base::num::bounded, buffer::bytes::bounded::MAX};

#[derive(Clone, Copy, Debug)]
pub struct Layout {
    entries: NonZero<u16>,
    capacity: NonZero<u32>,
}

impl Layout {
    pub const CANARY: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(3);

    pub const fn fixed<const ENTRIES: u16, const BYTES: u32>() -> Self {
        const {
            assert!(ENTRIES >= 2 && ENTRIES <= 32_768);
            assert!(BYTES > 0 && BYTES as usize <= MAX);
            assert!(ENTRIES as usize <= isize::MAX as usize / BYTES as usize);
            Self {
                entries: NonZero::<u16>::MIN.saturating_add(ENTRIES - 1),
                capacity: NonZero::<u32>::MIN.saturating_add(BYTES - 1),
            }
        }
    }
}

// SAFETY: construction checks the logical count, ABI bounds and mapped extent.
unsafe impl memory::raw::Layout for Layout {
    fn entries(self) -> NonZero<u16> {
        self.entries
    }
    fn capacity(self) -> NonZero<u32> {
        self.capacity
    }
}

// SAFETY: the typed limit is the exact capacity stored by the layout.
unsafe impl memory::raw::Receive for Layout {
    type Extent = Self;
    fn limit(self) -> NonZero<u32> {
        self.capacity
    }
}

// SAFETY: the fixed two-entry layout has one native scalar in each slot.
unsafe impl memory::raw::Canary for Layout {
    const LAYOUT: Self = Self::fixed::<2, 4>();
}

// SAFETY: bounded lengths are returned unchanged and capacities remain nonzero.
unsafe impl memory::raw::Extent for Layout {
    type Length = bounded::U32<0, { MAX as u32 }>;
    type Capacity = NonZero<u32>;
}
