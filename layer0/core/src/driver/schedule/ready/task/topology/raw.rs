use std::mem;

use o3::mem::Lease;

use crate::driver::schedule::ready;

/// Lossless stored form of one topology-owned local identity.
///
/// # Safety
/// `raw` and `from_raw` must preserve the exact typed value without validation.
pub unsafe trait Index: Copy + Eq {
    fn raw(self) -> u32;

    unsafe fn from_raw(raw: u32) -> Self;
}

pub(super) fn restore<I: Index>(raw: u32) -> I {
    // SAFETY: every caller receives the raw value from this topology's checked storage.
    unsafe { I::from_raw(raw) }
}

const _: () = {
    assert!(mem::size_of::<Lease<ready::TopologyKey<'static>>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Lease<ready::TopologyKey<'static>>>() == mem::align_of::<usize>());
};
