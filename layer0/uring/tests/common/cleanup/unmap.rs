use crate::common::cleanup::schedule::Schedule;

/// # Safety
/// The address and length satisfy the native unmap contract.
#[unsafe(export_name = "munmap")]
pub unsafe extern "C" fn release(address: *mut libc::c_void, length: usize) -> libc::c_int {
    match Schedule::intercept(address, length) {
        Some(code) => {
            // SAFETY: libc returns this thread's writable errno slot.
            unsafe { *libc::__errno_location() = code.get() };
            -1
        }
        None => {
            // SAFETY: the native entry transfers the unchanged unmap ABI and arguments.
            unsafe { libc::syscall(libc::SYS_munmap, address, length) as libc::c_int }
        }
    }
}
