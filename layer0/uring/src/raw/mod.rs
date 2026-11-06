mod create;
mod dir;
mod open;
mod source;

use std::{mem, num::NonZero};

use dope_sys::io::file;
use o3::base::num::bounded;

use crate::Cpus;

#[repr(transparent)]
pub struct Metadata(pub(crate) mem::MaybeUninit<libc::statx>);

impl Metadata {
    pub const UNINIT: Self = Self(mem::MaybeUninit::uninit());
    pub(crate) const MASK: u32 = libc::STATX_TYPE | libc::STATX_SIZE;

    /// # Safety
    /// A successful native operation initialized this output and ended access.
    pub unsafe fn try_result(&self) -> Result<(file::Kind, file::Offset), file::MetadataError> {
        // SAFETY: the successful native writer initialized the complete statx.
        let raw = unsafe { self.0.assume_init_ref() };
        if let Some(missing) = NonZero::new(Self::MASK & !raw.stx_mask) {
            return Err(file::MetadataError::Missing(missing));
        }
        let size =
            file::Offset::new(raw.stx_size).ok_or(file::MetadataError::Overflow(raw.stx_size))?;
        Ok((file::Kind::from_mode(raw.stx_mode.into()), size))
    }
}

const _: () = {
    assert!(mem::size_of::<Metadata>() == 256);
    assert!(mem::align_of::<Metadata>() == 8);
};

/// Retains the native mask index already admitted by the CPU iterator.
/// # Safety
/// `index` was yielded by this backend's range bounded by `Cpus::LIMIT`.
pub(crate) unsafe fn cpu(index: u16) -> bounded::U32<0, { Cpus::LIMIT as u32 - 1 }> {
    // SAFETY: the caller retains the iterator's exact native mask bound.
    unsafe { bounded::raw::u32(u32::from(index)) }
}
