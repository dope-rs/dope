use std::io;
use std::mem::zeroed;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};

use crate::DriverContext;
use dope_core::driver::control::ContextControl;

struct MaskGuard {
    previous: libc::sigset_t,
}

impl Drop for MaskGuard {
    fn drop(&mut self) {
        let result = unsafe {
            libc::pthread_sigmask(
                libc::SIG_SETMASK,
                &self.previous,
                std::ptr::null_mut(),
            )
        };
        if result != 0 {
            std::process::abort();
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
        let mut set: libc::sigset_t = unsafe { zeroed() };
        if unsafe { libc::sigemptyset(&mut set) } != 0
            || unsafe { libc::sigaddset(&mut set, libc::SIGINT) } != 0
            || unsafe { libc::sigaddset(&mut set, libc::SIGTERM) } != 0
        {
            return Err(io::Error::last_os_error());
        }

        let mut previous: libc::sigset_t = unsafe { zeroed() };
        let result = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut previous) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result));
        }
        let mask = MaskGuard { previous };

        let raw = unsafe { libc::signalfd(-1, &set, libc::SFD_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
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
        unsafe { driver.register_shutdown_fd(fd.as_fd()) }
    }
}
