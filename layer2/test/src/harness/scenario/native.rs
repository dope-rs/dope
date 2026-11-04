pub(super) fn terminate() -> ! {
    // SAFETY: SIGKILL targets this watchdog thread and terminates its process;
    // _exit is the fallback if raising the signal fails.
    unsafe {
        libc::raise(libc::SIGKILL);
        libc::_exit(1)
    }
}
