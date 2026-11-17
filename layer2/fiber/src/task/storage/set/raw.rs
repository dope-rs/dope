use std::pin::Pin;

use o3::depot::{self, arena::slab, index::Initialized};

use crate::{
    abi,
    task::storage::{
        fixed::schedule::ready::Global,
        set::{Pool, Value},
    },
};

/// # Safety
/// The scoped identity or paired live dequeue must retain this exact occupied slot.
pub(super) unsafe fn entry<'a, 'd, F: abi::Fiber<'d>, T, D>(
    slots: Pin<&'a mut Pool<'d, F, T, D>>,
    index: Global,
) -> slab::Anchored<'a, Value<'d, F, T>, D, 1, true, 1, slab::storage::Lazy> {
    // SAFETY: the exclusive owner loan retains occupancy and the admitted index range.
    unsafe { depot::raw::Proof::<_, Initialized<()>>::new((slots, index.get() as u32)) }.into()
}
