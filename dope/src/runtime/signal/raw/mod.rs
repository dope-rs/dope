use std::io::{self, Error};
use std::mem::zeroed;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::process::abort;
use std::ptr::null_mut;

use crate::DriverContext;
use dope_core::driver::control::ContextControl;
use libc::{
    pthread_sigmask, sigaddset, sigemptyset, signalfd, sigset_t, SFD_CLOEXEC, SIGINT, SIGTERM,
    SIG_BLOCK, SIG_SETMASK,
};

struct MaskGuard {
    previous: sigset_t,
}

impl Drop for MaskGuard {
    fn drop(&mut self) {
        let result = unsafe { pthread_sigmask(SIG_SETMASK, &self.previous, null_mut()) };
        if result != 0 {
            abort();
        }
    }
}

pub(in crate::runtime) struct SignalState {
    // Fields drop in declaration order: close the signal fd before restoring the mask.
    fd: OwnedFd,
    _mask: MaskGuard,
}

impl SignalState {
    pub(in crate::runtime) fn new() -> io::Result<Self> {
        let mut set: sigset_t = unsafe { zeroed() };
        if unsafe { sigemptyset(&mut set) } != 0
            || unsafe { sigaddset(&mut set, SIGINT) } != 0
            || unsafe { sigaddset(&mut set, SIGTERM) } != 0
        {
            return Err(Error::last_os_error());
        }

        let mut previous: sigset_t = unsafe { zeroed() };
        let result = unsafe { pthread_sigmask(SIG_BLOCK, &set, &mut previous) };
        if result != 0 {
            return Err(Error::from_raw_os_error(result));
        }
        let mask = MaskGuard { previous };

        let raw = unsafe { signalfd(-1, &set, SFD_CLOEXEC) };
        if raw < 0 {
            return Err(Error::last_os_error());
        }

        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(raw) },
            _mask: mask,
        })
    }

    pub(in crate::runtime) fn try_register(
        &self,
        driver: &mut DriverContext<'_, '_>,
    ) -> io::Result<()> {
        driver.register_shutdown_fd(self.fd.as_fd())
    }
}
