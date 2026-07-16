use std::io;

cfg_select! {
    target_os = "linux" => {
        use std::mem::zeroed;
        use std::os::fd::{AsFd, BorrowedFd, FromRawFd, OwnedFd};

        pub(super) struct SignalState {
            fd: Option<OwnedFd>,
            previous: libc::sigset_t,
        }

        impl SignalState {
            pub(super) fn new() -> io::Result<Self> {
                // SAFETY: sigset_t is plain old data; zeroed is a valid initial value.
                let mut set: libc::sigset_t = unsafe { zeroed() };
                // SAFETY: set is a live local.
                if unsafe { libc::sigemptyset(&mut set) } != 0
                    || unsafe { libc::sigaddset(&mut set, libc::SIGINT) } != 0
                    || unsafe { libc::sigaddset(&mut set, libc::SIGTERM) } != 0
                {
                    return Err(io::Error::last_os_error());
                }

                // SAFETY: sigset_t is plain old data; zeroed is a valid initial value.
                let mut previous: libc::sigset_t = unsafe { zeroed() };
                // SAFETY: set and previous are live locals.
                let mask_result = unsafe {
                    libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut previous)
                };
                if mask_result != 0 {
                    return Err(io::Error::from_raw_os_error(mask_result));
                }

                // SAFETY: set is a live local; -1 requests a new fd.
                let raw = unsafe { libc::signalfd(-1, &set, libc::SFD_CLOEXEC) };
                if raw < 0 {
                    let error = io::Error::last_os_error();
                    // SAFETY: previous holds the mask captured above.
                    unsafe {
                        libc::pthread_sigmask(
                            libc::SIG_SETMASK,
                            &previous,
                            std::ptr::null_mut(),
                        );
                    }
                    return Err(error);
                }

                Ok(Self {
                    // SAFETY: raw is a fresh signalfd we own.
                    fd: Some(unsafe { OwnedFd::from_raw_fd(raw) }),
                    previous,
                })
            }

            pub(super) fn fd(&self) -> BorrowedFd<'_> {
                self.fd.as_ref().expect("signal fd is live").as_fd()
            }
        }

        impl Drop for SignalState {
            fn drop(&mut self) {
                drop(self.fd.take());
                // SAFETY: previous holds the mask captured at construction.
                let result = unsafe {
                    libc::pthread_sigmask(
                        libc::SIG_SETMASK,
                        &self.previous,
                        std::ptr::null_mut(),
                    )
                };
                debug_assert_eq!(result, 0, "failed to restore the thread signal mask");
            }
        }
    }
    _ => {
        pub(super) struct SignalState;

        impl SignalState {
            pub(super) fn new() -> io::Result<Self> {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "signal shutdown is only available on Linux",
                ))
            }
        }
    }
}
