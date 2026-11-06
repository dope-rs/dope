use std::{
    num::NonZeroI32,
    os::{fd, fd::AsRawFd as _},
};

use dope_sys::engine::register;

/// Receipt whose owner keeps this exact descriptor open until it is dropped.
pub(super) struct Descriptor(NonZeroI32, register::Interest);

impl Descriptor {
    /// Records a descriptor retained by the enclosing source owner.
    /// # Safety
    /// `source` must remain open until this receipt is dropped.
    pub(super) unsafe fn new(source: register::Source<'_>) -> Self {
        // SAFETY: BorrowedFd excludes -1, so its bitwise complement is nonzero.
        Self(
            unsafe { NonZeroI32::new_unchecked(!source.fd.as_raw_fd()) },
            source.interest,
        )
    }

    pub(super) fn source(&self) -> register::Source<'_> {
        // SAFETY: the nonzero complement excludes -1; source custody keeps the
        // descriptor open throughout this receipt's borrow.
        register::Source {
            fd: unsafe { fd::BorrowedFd::borrow_raw(!self.0.get()) },
            interest: self.1,
        }
    }
}
