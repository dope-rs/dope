use std::io;
use std::mem::zeroed;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};

use crate::DriverContext;
use dope_core::driver::control::ContextControl;
use std::io::Error;
use libc::SFD_CLOEXEC;
use libc::SIGINT;
use libc::SIGTERM;
use libc::SIG_BLOCK;
use libc::SIG_SETMASK;
use std::process::abort;
use std::ptr::null_mut;
use libc::pthread_sigmask;
use libc::sigaddset;
use libc::sigemptyset;
use libc::signalfd;
use libc::sigset_t;

struct MaskGuard {
    previous: sigset_t,
}

impl Drop for MaskGuard {
    fn drop(&mut self) {
        let result = unsafe {
            pthread_sigmask(
                SIG_SETMASK,
                &self.previous,
                null_mut(),
            )
        };
        if result != 0 {
            abort();
        }
    }
}

pub(in crate::runtime) struct SignalState {
    fd: Option<OwnedFd>,
    _mask: MaskGuard,
}

impl Drop for SignalState {
    fn drop(&mut self) {
        drop(self.fd.take());
    }
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
            fd: Some(unsafe { OwnedFd::from_raw_fd(raw) }),
            _mask: mask,
        })
    }

    pub(in crate::runtime) fn try_register(
        &self,
        driver: &mut DriverContext<'_, '_>,
    ) -> io::Result<()> {
        let fd = self.fd.as_ref().expect("live signal state must own its fd");
        driver.register_shutdown_fd(fd.as_fd())
    }
}
