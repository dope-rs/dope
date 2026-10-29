mod create;
mod dir;
mod open;
mod receive;
mod retain;
mod source;

use dope_sys::kernel;

use crate::Kqueue;

// SAFETY: reads expose this thread's unchanged native errno slot directly.
unsafe impl kernel::raw::Source for Kqueue {
    fn read() -> i32 {
        // SAFETY: this native TLS cell is read synchronously on its owning thread.
        unsafe { libc::__error().read() }
    }
}
