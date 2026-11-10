use std::pin::Pin;

use o3::{
    base::num::Epoch,
    depot::{self, arena::slab::external, index::Initialized, raw::Compact},
};

use crate::{
    core::driver::schedule::ready::task::topology, task::storage::fixed::schedule::ready::Global,
};

/// # Safety
/// The index must retain an occupied admission to this exact storage's queue.
pub(in crate::task::storage::fixed::schedule) unsafe fn entry<T>(
    slots: Pin<&mut external::Pinned<T, Epoch>>,
    index: Global,
) -> external::Anchored<'_, T, Epoch> {
    unsafe { depot::raw::Proof::<_, Initialized<()>>::new((slots, index.get() as u32)) }.into()
}

/// # Safety
/// The index must retain an occupied admission to this exact storage's queue.
pub(in crate::task::storage::fixed::schedule) unsafe fn get<T>(
    slots: &external::Pinned<T, Epoch>,
    index: Global,
) -> &T {
    unsafe { depot::raw::Proof::<_, Initialized<()>>::new((slots, index.get() as u32)) }.into()
}

unsafe impl topology::raw::Index for Global {
    fn raw(self) -> u32 {
        self.0.raw()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        Self(unsafe { Compact::from_raw(raw) })
    }
}
