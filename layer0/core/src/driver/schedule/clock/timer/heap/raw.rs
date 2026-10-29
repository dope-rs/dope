use std::{cell, marker, mem};

use o3::{alloc, base::marker::ThreadBound};

use crate::driver::schedule::clock::timer::heap::{Core, Min};

/// Persistent branded heap with non-Copy residents and their Drop obligations.
/// Preserves the four-plane layout, six-word owner, and 128-byte resident alignment.
/// Loans are allocation-free and exclusively borrow this store's retained state.
#[repr(transparent)]
pub struct Store<P: Ord, R, Tag = ()> {
    inner: cell::UnsafeCell<Core<P, R>>,
    tag: marker::PhantomData<cell::Cell<Tag>>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<P: Ord, R, Tag> Store<P, R, Tag> {
    /// Allocates the fixed-capacity store inside its persistent owner cell.
    pub fn try_new(cap: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: cell::UnsafeCell::new(Core::try_new(cap)?),
            tag: marker::PhantomData,
            thread: marker::PhantomData,
        })
    }

    /// Runs `op` with one non-escaping exclusive loan.
    ///
    /// # Safety
    /// Every lease used by `op` must originate from `self`, and no other loan may overlap `op`.
    pub unsafe fn loan<'id, T>(
        &'id self,
        op: impl for<'a> FnOnce(Min<'a, 'id, P, R, Tag>) -> T,
    ) -> T {
        op(Min::new(
            // SAFETY: the caller guarantees no overlapping loan, and the
            // higher-ranked callback cannot return this exclusive core borrow.
            unsafe { &mut *self.inner.get() },
            marker::PhantomData,
        ))
    }
}

const _: () = {
    assert!(mem::size_of::<Store<u128, u64>>() == 6 * mem::size_of::<usize>());
    assert!(mem::align_of::<Store<u128, u64>>() == mem::align_of::<usize>());
};
