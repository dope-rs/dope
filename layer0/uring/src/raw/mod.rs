mod create;
mod dir;
mod open;
mod source;

use o3::base::num::bounded;

use crate::Cpus;

/// Retains the native mask index already admitted by the CPU iterator.
/// # Safety
/// `index` was yielded by this backend's range bounded by `Cpus::LIMIT`.
pub(crate) unsafe fn cpu(index: u16) -> bounded::U32<0, { Cpus::LIMIT as u32 - 1 }> {
    // SAFETY: the caller retains the iterator's exact native mask bound.
    unsafe { bounded::raw::Proof::assume(u32::from(index)) }.retain()
}
