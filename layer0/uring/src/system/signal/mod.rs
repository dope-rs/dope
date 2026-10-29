use std::{marker, mem, os::fd};

pub(crate) use acquire::Acquire;
use dope_sys::{Errno, kernel, thread::signal};
pub(crate) use wait::Wait;

use crate::Uring;

mod acquire;
pub(crate) mod raw;
mod wait;

#[repr(transparent)]
pub struct Source<'a> {
    fd: fd::BorrowedFd<'a>,
    owner: marker::PhantomData<&'a raw::Receiver>,
}

impl<'a> Source<'a> {
    fn new(owner: &'a raw::Receiver) -> Self {
        Self {
            fd: fd::AsFd::as_fd(owner),
            owner: marker::PhantomData,
        }
    }
}

impl signal::Signal for Source<'_> {
    type Error = Wait;
    fn wait(&self) -> Result<(), Wait> {
        use std::os::fd::{AsFd as _, AsRawFd as _};

        let mut info = mem::MaybeUninit::<libc::signalfd_siginfo>::uninit();
        loop {
            // SAFETY: source borrows a live SIGTERM receiver and info is writable.
            let len = unsafe {
                libc::read(
                    self.as_fd().as_raw_fd(),
                    info.as_mut_ptr().cast(),
                    mem::size_of::<libc::signalfd_siginfo>(),
                )
            };
            // SAFETY: read just returned on this thread; `ssize_t` and `c_long`
            // are the same native signed word, pinned by `Uring::status`, and one
            // signalfd read fills exactly the retained siginfo ABI.
            const SIZE: libc::c_int = mem::size_of::<libc::signalfd_siginfo>() as libc::c_int;
            let status = unsafe { Uring::status(len as libc::c_long) };
            // SAFETY: one signalfd read of the retained siginfo returns exactly SIZE or -1.
            let exact = unsafe { status.admit_exact::<SIZE>() };
            // SAFETY: the read above just returned on this thread.
            match unsafe { exact.try_admit::<Errno>(|| kernel::raw::Code::last::<Uring>().into()) }
            {
                Ok(()) => return Ok(()),
                Err(fault) if fault.is_intr() => continue,
                Err(fault) => return Err(Wait::Read(fault)),
            }
        }
    }
}

impl fd::AsFd for Source<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd
    }
}

const _: () = {
    assert!(mem::size_of::<Source<'_>>() == mem::size_of::<fd::BorrowedFd<'_>>());
    assert!(mem::size_of::<Option<Source<'_>>>() == mem::size_of::<Option<fd::BorrowedFd<'_>>>());
};
