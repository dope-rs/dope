use crate::{io::socket::msg, kernel};
/// # Safety
/// The header and all pointees stay initialized, immutable, and live through `'a`.
/// Its iovec count fits the target and its byte sum is at most `i32::MAX`.
pub unsafe trait Message<'a> {
    fn header(self) -> &'a msg::Header;
}

/// A byte range that retains its owner through unchecked prefix projection.
///
/// # Safety
/// `prefix` must preserve the original owner and its admitted byte extent.
pub unsafe trait Buffer: Sized {
    /// # Safety
    /// `len` must not exceed this buffer's admitted extent.
    unsafe fn prefix(self, len: usize) -> Self;

    /// Decodes an admitted completion, reading `errno` only for minus one.
    ///
    /// # Safety
    /// `raw` is -1 or a nonnegative count within this owner's admitted extent.
    /// On failure, `S` reads this call's positive, unchanged errno.
    unsafe fn try_native<S: kernel::raw::Source + ?Sized, E: From<crate::Errno>>(
        self,
        raw: libc::ssize_t,
    ) -> Result<Self, E> {
        if raw == -1 {
            // SAFETY: the caller retains this failed call's positive, unchanged errno.
            return Err(E::from(unsafe { kernel::raw::Code::last::<S>() }.into()));
        }
        // SAFETY: the caller retains this completed count's admitted extent proof.
        Ok(unsafe { self.prefix(raw as usize) })
    }
}

// SAFETY: slice length is its initialized byte extent.
unsafe impl Buffer for &[u8] {
    unsafe fn prefix(self, len: usize) -> Self {
        unsafe { self.get_unchecked(..len) }
    }
}

// SAFETY: the exclusive slice retains its initialized byte extent.
unsafe impl Buffer for &mut [u8] {
    unsafe fn prefix(self, len: usize) -> Self {
        unsafe { self.get_unchecked_mut(..len) }
    }
}

// SAFETY: the exclusive slice retains its writable byte extent.
unsafe impl Buffer for &mut [std::mem::MaybeUninit<u8>] {
    unsafe fn prefix(self, len: usize) -> Self {
        unsafe { self.get_unchecked_mut(..len) }
    }
}
