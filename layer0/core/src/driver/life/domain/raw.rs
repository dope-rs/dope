use std::{
    num::NonZeroI32,
    os::{fd, fd::AsRawFd as _},
};

/// Receipt whose owner keeps this exact descriptor open until it is dropped.
pub(super) struct Descriptor(NonZeroI32);

impl Descriptor {
    /// Records a descriptor retained by the enclosing source owner.
    /// # Safety
    /// `source` must remain open until this receipt is dropped.
    pub(super) unsafe fn new(source: fd::BorrowedFd<'_>) -> Self {
        // SAFETY: BorrowedFd excludes -1, so its bitwise complement is nonzero.
        Self(unsafe { NonZeroI32::new_unchecked(!source.as_raw_fd()) })
    }
}

impl fd::AsFd for Descriptor {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        // SAFETY: the nonzero complement excludes -1; source custody keeps the
        // descriptor open throughout this receipt's borrow.
        unsafe { fd::BorrowedFd::borrow_raw(!self.0.get()) }
    }
}
