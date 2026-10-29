use std::{marker, mem};

use o3::{alloc, depot::capacity};

use crate::driver::schedule::clock::timer::heap::core::Resident;

#[repr(transparent)]
pub(super) struct Capacity<P, R> {
    count: capacity::Count,
    ownership: marker::PhantomData<(P, R)>,
}

impl<P, R> Capacity<P, R> {
    pub(super) fn new(capacity: usize) -> Result<Self, alloc::Overflow> {
        use alloc::raw::Rows;

        if capacity > (u32::MAX - 2) as usize || capacity > Self::EXTRA_MAX {
            return Err(alloc::Overflow);
        }
        Ok(Self {
            count: capacity::Count::new(capacity as u32),
            ownership: marker::PhantomData,
        })
    }

    pub(super) const fn count(&self) -> capacity::Count {
        self.count
    }
}

// SAFETY: construction proves the slot domain and all four immutable planes.
unsafe impl<P, R> alloc::raw::Shape for Capacity<P, R> {
    type Root = u32;
    type Tail = mem::MaybeUninit<P>;
    type Extra = u32;
    type Last = Resident<R>;

    fn len(&self) -> usize {
        self.count.get()
    }
}

// SAFETY: Shape retains one count for all four planes and uses its default
// tail_len; construction admits that count against the complete EXTRA_MAX.
unsafe impl<P, R> alloc::raw::Rows for Capacity<P, R> {}
