unsafe extern "C" {
    fn getpid() -> std::ffi::c_int;
    fn kill(pid: std::ffi::c_int, signal: std::ffi::c_int) -> std::ffi::c_int;
    fn _exit(status: std::ffi::c_int) -> !;
}

pub(super) fn terminate() -> ! {
    // SAFETY: the watchdog terminates its own process with SIGKILL; _exit is the fallback.
    unsafe {
        kill(getpid(), 9);
        _exit(1)
    }
}
