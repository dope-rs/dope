use std::{mem, os::fd, ptr};

use dope_sys::{Errno, kernel, thread::signal};
use o3::base::marker::ThreadBound;

use crate::{
    Uring,
    system::signal::{Acquire, Source, Wait},
};

#[derive(Debug)]
pub struct Receiver {
    fd: fd::OwnedFd,
    previous: mem::MaybeUninit<libc::sigset_t>,
    _thread: ThreadBound,
}

impl Receiver {
    fn mask() -> libc::sigset_t {
        let mut mask = mem::MaybeUninit::<libc::sigset_t>::zeroed();
        // SAFETY: mask names writable signal-set storage; sigemptyset succeeds for a nonnull set.
        unsafe { libc::sigemptyset(mask.as_mut_ptr()) };
        // SAFETY: sigemptyset established an empty native set and SIGTERM is valid.
        unsafe { libc::sigaddset(mask.as_mut_ptr(), libc::SIGTERM) };
        // SAFETY: zeroed initialized the full Rust representation; both calls preserve it.
        unsafe { mask.assume_init() }
    }
}

impl fd::AsFd for Receiver {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

const _: () = {
    assert!(mem::size_of::<Option<Receiver>>() == mem::size_of::<Receiver>());
};

impl signal::raw::Receiver for Receiver {
    type Acquire = Acquire;
    type Wait = Wait;
    type Source<'a> = Source<'a>;

    unsafe fn try_new() -> Result<Self, Acquire> {
        use std::os::fd::OwnedFd;

        use dope_sys::io::fd;
        let mask = Self::mask();
        // SAFETY: mask contains SIGTERM and -1 requests a new descriptor.
        let raw = unsafe { libc::signalfd(-1, &mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK) };
        // SAFETY: signalfd returns a fresh descriptor or -1 with unchanged errno.
        let fd = unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt::<Errno>(raw, || {
                kernel::raw::Code::last::<Uring>().into()
            })
        }
        .map_err(Acquire::SignalFd)?;
        let mut previous = mem::MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: mask is initialized and previous is writable for one set.
        // Blocking last leaves the original mask unchanged on every acquisition failure.
        let result =
            unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &mask, previous.as_mut_ptr()) };
        // SAFETY: pthread_sigmask returns zero or a positive error code.
        unsafe { kernel::raw::Status::new(result).try_pthread() }.map_err(Acquire::MaskBlock)?;
        Ok(Self {
            fd,
            previous,
            _thread: ThreadBound::NEW,
        })
    }

    fn source(&self) -> Self::Source<'_> {
        Source::new(self)
    }

    fn restore(&self) -> Result<(), signal::ReleaseFault> {
        // SAFETY: acquisition stored this thread's native mask for pointer-only reuse.
        // ThreadBound preserves its thread and the caller retains exclusive signal ownership.
        let result = unsafe {
            libc::pthread_sigmask(libc::SIG_SETMASK, self.previous.as_ptr(), ptr::null_mut())
        };
        // SAFETY: pthread_sigmask returns zero or a positive error code.
        unsafe { kernel::raw::Status::new(result).try_pthread() }
            .map_err(signal::ReleaseFault::Mask)
    }
}
