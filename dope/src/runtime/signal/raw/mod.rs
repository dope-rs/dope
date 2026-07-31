use std::cell::Cell;
use std::io::{self, Error, ErrorKind};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::process::abort;
use std::ptr::null_mut;

use crate::DriverContext;
use dope_core::driver::control::ContextControl;
use libc::{
    SFD_CLOEXEC, SIG_BLOCK, SIG_SETMASK, SIGINT, SIGTERM, c_int, pthread_sigmask, sigaddset,
    sigemptyset, signalfd, sigset_t,
};

std::thread_local! {
    static SIGNAL_LEASED: Cell<bool> = const { Cell::new(false) };
}

struct SignalLease;

impl SignalLease {
    fn acquire() -> io::Result<Self> {
        SIGNAL_LEASED.with(|active| {
            if active.replace(true) {
                Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "dope: signal shutdown already active on this thread",
                ))
            } else {
                Ok(Self)
            }
        })
    }
}

impl Drop for SignalLease {
    fn drop(&mut self) {
        SIGNAL_LEASED.with(|active| {
            if !active.replace(false) {
                abort();
            }
        });
    }
}

struct SignalSet(sigset_t);

impl SignalSet {
    fn from_signals<const N: usize>(signals: [c_int; N]) -> io::Result<Self> {
        let mut set = MaybeUninit::<sigset_t>::uninit();
        // SAFETY: sigemptyset initializes the complete sigset_t before it is
        // assumed initialized; subsequent calls receive that live value.
        unsafe {
            if sigemptyset(set.as_mut_ptr()) != 0 {
                return Err(Error::last_os_error());
            }
            let mut set = set.assume_init();
            for signal in signals {
                if sigaddset(&mut set, signal) != 0 {
                    return Err(Error::last_os_error());
                }
            }
            Ok(Self(set))
        }
    }

    fn as_ptr(&self) -> *const sigset_t {
        &raw const self.0
    }

    fn open_fd(&self) -> io::Result<OwnedFd> {
        // SAFETY: self remains live for the call. A successful signalfd return
        // transfers one newly owned descriptor to OwnedFd.
        unsafe {
            let raw = signalfd(-1, self.as_ptr(), SFD_CLOEXEC);
            if raw < 0 {
                return Err(Error::last_os_error());
            }
            Ok(OwnedFd::from_raw_fd(raw))
        }
    }
}

struct BlockedMask {
    previous: sigset_t,
    _lease: SignalLease,
}

impl BlockedMask {
    fn acquire(set: &SignalSet, lease: SignalLease) -> io::Result<Self> {
        let mut previous = MaybeUninit::<sigset_t>::uninit();
        // SAFETY: both pointers name valid sigset_t storage. On success,
        // pthread_sigmask initializes the complete previous mask.
        let previous = unsafe {
            let result = pthread_sigmask(SIG_BLOCK, set.as_ptr(), previous.as_mut_ptr());
            if result != 0 {
                return Err(Error::from_raw_os_error(result));
            }
            previous.assume_init()
        };
        Ok(Self {
            previous,
            _lease: lease,
        })
    }
}

impl Drop for BlockedMask {
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
    _mask: BlockedMask,
}

impl SignalState {
    pub(in crate::runtime) fn new() -> io::Result<Self> {
        let lease = SignalLease::acquire()?;
        let set = SignalSet::from_signals([SIGINT, SIGTERM])?;
        let mask = BlockedMask::acquire(&set, lease)?;
        let fd = set.open_fd()?;

        Ok(Self {
            fd,
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
