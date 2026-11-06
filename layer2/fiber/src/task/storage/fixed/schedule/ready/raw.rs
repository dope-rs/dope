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
/// exact R owner. Layout and admission prove its physical index.
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

unsafe impl topology::raw::Index for Global {
    fn raw(self) -> u32 {
        !self.0.get()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: the offset contract excludes the compact sentinel.
        Self(unsafe { NonZero::new_unchecked(!raw) })
    }
}
