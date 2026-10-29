use std::{mem, num::NonZero};

use crate::{Errno, kernel};

/// Native thread attributes carrying one validated stack extent.
pub struct Attrs(libc::pthread_attr_t);

impl Attrs {
    /// Initializes attributes for a thread with the exact requested stack.
    pub fn with_stack(stack: NonZero<usize>) -> Result<Self, Errno> {
        let mut raw = mem::MaybeUninit::uninit();
        // SAFETY: raw has the native attribute size and is initialized on success.
        let result = unsafe { libc::pthread_attr_init(raw.as_mut_ptr()) };
        // SAFETY: pthread_attr_init returns zero or a positive error code directly.
        unsafe { kernel::raw::Status::new(result).try_pthread() }?;
        // SAFETY: pthread_attr_init completed, so this value is initialized and
        // owned here until its Drop destroys it.
        let mut attr = Self(unsafe { raw.assume_init() });
        let result = unsafe { libc::pthread_attr_setstacksize(&mut attr.0, stack.get()) };
        // SAFETY: pthread_attr_setstacksize returns zero or a positive error code.
        unsafe { kernel::raw::Status::new(result).try_pthread() }?;
        Ok(attr)
    }

    pub(super) fn as_ptr(&self) -> *const libc::pthread_attr_t {
        &self.0
    }
}

/// Destroys the uniquely owned attributes; no caller can invalidate them first.
impl Drop for Attrs {
    fn drop(&mut self) {
        // SAFETY: these initialized attributes are exclusively owned and destroyed
        // once. Only stack size was changed, so no work-interval port is retained;
        // destruction returns exactly zero on the admitted native implementations.
        unsafe { libc::pthread_attr_destroy(&mut self.0) };
    }
}
