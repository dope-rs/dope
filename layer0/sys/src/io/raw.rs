use crate::io::socket::msg;
/// # Safety
/// The header and all pointees stay initialized, immutable, and live through `'a`.
/// Its iovec count fits the target and its byte sum is at most `i32::MAX`.
pub unsafe trait Message<'a> {
    fn header(self) -> &'a msg::Header;
}

/// A byte range that retains its owner through unchecked prefix projection.
///
/// # Safety
/// `prefix` must preserve the original owner and its admitted initialized bytes.
pub unsafe trait Buffer: Sized {
    /// # Safety
    /// `len` must not exceed this buffer's initialized extent.
    unsafe fn prefix(self, len: usize) -> Self;

    /// Decodes an admitted completion, reading `errno` only for minus one.
    ///
    /// # Safety
    /// `raw` is -1 or a nonnegative count within this owner's initialized extent.
    unsafe fn try_native<E: From<crate::Errno>>(
        self,
        raw: libc::ssize_t,
        errno: impl FnOnce() -> crate::Errno,
    ) -> Result<Self, E> {
        if raw == -1 {
            return Err(E::from(errno()));
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
