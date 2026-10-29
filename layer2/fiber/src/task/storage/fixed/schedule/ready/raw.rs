use std::{marker, num::NonZero};

use crate::{
    core::driver::schedule::ready::task::topology,
    task::storage::fixed::{
        schedule::ready::{Global, TreeIndex},
        tree,
    },
};

/// # Safety
/// `global` must come from a live admission in the queue paired with this
/// exact R owner. Path admission proves the index is within R::TASKS.
pub(in crate::task::storage::fixed::schedule) unsafe fn admitted<'d, R>(
    global: Global,
) -> TreeIndex<R>
where
    R: tree::Tree<'d>,
{
    TreeIndex {
        index: global,
        tree: marker::PhantomData,
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::task::storage::fixed::schedule) struct Local<const N: usize>(NonZero<u32>);

impl<const N: usize> Local<N> {
    /// # Safety
    /// `index < N` and `N <= u32::MAX` must be proved by the owning pool.
    pub(in crate::task::storage::fixed::schedule) unsafe fn from_raw(index: u32) -> Self {
        Self(unsafe { NonZero::new_unchecked(!index) })
    }

    pub(in crate::task::storage::fixed::schedule) const fn raw(self) -> u32 {
        !self.0.get()
    }
}

unsafe impl topology::raw::Index for Global {
    fn raw(self) -> u32 {
        !self.0.get()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: the offset contract excludes the compact sentinel.
        Self(unsafe { NonZero::new_unchecked(!raw) })
    }
}
