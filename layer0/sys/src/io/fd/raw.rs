use std::os::fd;

use crate::{Errno, kernel};

/// Fallible descriptor decoding preserving the standard ownership contract.
pub trait Adopt: fd::FromRawFd + Sized {
    /// Decode a syscall that transfers one descriptor or returns minus one.
    ///
    /// # Safety
    /// `raw` is -1 or transfers ownership of one live nonnegative descriptor.
    unsafe fn try_adopt<E: From<Errno>>(
        raw: fd::RawFd,
        errno: impl FnOnce() -> Errno,
    ) -> Result<Self, E> {
        // SAFETY: the caller retains the native descriptor-or-minus-one return proof.
        let value = unsafe { kernel::raw::Status::new(raw).try_value::<E>(errno) }?;
        // SAFETY: the nonnegative syscall result transfers this descriptor.
        Ok(unsafe { Self::from_raw_fd(value.get()) })
    }
}

impl<T: fd::FromRawFd> Adopt for T {}

/// The exact two descriptors one native pair call writes.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Pair([fd::RawFd; 2]);

impl Pair {
    /// Retains one native pair call's exact writable destination.
    pub const fn new(raw: [fd::RawFd; 2]) -> Self {
        Self(raw)
    }

    /// Adopts both fresh descriptors one successful native pair call wrote.
    ///
    /// # Safety
    /// `raw` is that call's unchanged return and this is its exact destination.
    pub unsafe fn try_adopt<E: From<Errno>>(
        self,
        raw: libc::c_int,
        errno: impl FnOnce() -> Errno,
    ) -> Result<(fd::OwnedFd, fd::OwnedFd), E> {
        // SAFETY: every native producer of a descriptor pair returns exactly zero or -1.
        unsafe { kernel::raw::Status::new(raw).try_zero::<E>(errno) }?;
        let [left, right] = self.0;
        // SAFETY: that success transferred two distinct live descriptors to this caller once.
        Ok(unsafe {
            (
                <fd::OwnedFd as fd::FromRawFd>::from_raw_fd(left),
                <fd::OwnedFd as fd::FromRawFd>::from_raw_fd(right),
            )
        })
    }
}
