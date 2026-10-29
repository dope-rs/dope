use dope_sys::kernel;

use crate::Uring;

// SAFETY: read accesses the calling thread's actual native errno slot, preserving
// the value set by its immediately preceding syscall.
unsafe impl kernel::raw::Source for Uring {
    #[inline]
    fn read() -> i32 {
        // SAFETY: this native TLS cell is read synchronously on its owning thread.
        unsafe { libc::__errno_location().read() }
    }
}
