mod create;
mod dir;
mod open;
mod receive;
mod retain;
mod source;

use std::mem::MaybeUninit;

use dope_sys::{io::file, kernel};

use crate::Kqueue;

#[repr(transparent)]
pub struct Metadata(MaybeUninit<file::Metadata>);

impl Metadata {
    pub const UNINIT: Self = Self(MaybeUninit::uninit());

    pub(crate) fn write(&mut self, value: file::Metadata) {
        self.0.write(value);
    }

    /// # Safety
    /// The matching metadata operation succeeded and all native access to this output ended.
    pub unsafe fn try_result(&self) -> Result<(file::Kind, file::Offset), file::MetadataError> {
        // SAFETY: the caller retains this output's successful completion proof.
        let value = unsafe { self.0.assume_init_ref() };
        let size = value.size();
        let length = file::Offset::new(size as u64).ok_or(file::MetadataError::Negative(size))?;
        Ok((value.kind(), length))
    }
}

// SAFETY: reads expose this thread's unchanged native errno slot directly.
unsafe impl kernel::raw::Source for Kqueue {
    fn read() -> i32 {
        // SAFETY: this native TLS cell is read synchronously on its owning thread.
        unsafe { libc::__error().read() }
    }
}
