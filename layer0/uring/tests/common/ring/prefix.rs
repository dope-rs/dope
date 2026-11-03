use std::{mem, num::NonZero};

use dope_sys::{Errno, kernel::raw::Code};
use dope_uring::Uring;

/// # Safety
/// Own registered ring zero on this thread; publish `count` SQEs and retain
/// their backing storage through completion.
pub unsafe fn submit(count: NonZero<u32>) -> Result<u32, Errno> {
    const REGISTERED: u32 = 1 << 4;
    // SAFETY: the caller retains the registered ring and published requests;
    // this enter supplies no signal mask or extended argument pointers.
    let result = unsafe {
        libc::syscall(
            libc::SYS_io_uring_enter,
            0 as libc::c_long,
            count.get() as libc::c_long,
            0 as libc::c_long,
            REGISTERED as libc::c_long,
            0 as libc::c_long,
            mem::size_of::<libc::sigset_t>() as libc::c_long,
        )
    };
    if result < 0 {
        // SAFETY: the failed enter left this thread's positive errno unchanged.
        Err(unsafe { Code::last::<Uring>() }.into())
    } else {
        Ok(result as u32)
    }
}
