pub(super) fn terminate() -> ! {
    // SAFETY: the watchdog terminates its own process with SIGKILL; _exit is the fallback.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
        libc::_exit(1)
    }
}
