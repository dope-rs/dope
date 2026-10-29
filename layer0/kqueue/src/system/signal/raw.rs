use std::{mem, os::fd, ptr};

use dope_sys::{Errno, kernel, thread::signal};

use crate::{
    Kqueue,
    system::{
        Queue,
        signal::{Acquire, Source, Wait},
    },
};

#[derive(Debug)]
pub struct Receiver(Queue, mem::MaybeUninit<libc::sigaction>);

impl Receiver {
    fn register(queue: &crate::system::Queue) -> Result<(), Acquire> {
        let event = libc::kevent {
            ident: libc::SIGTERM as libc::uintptr_t,
            filter: libc::EVFILT_SIGNAL,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        };
        queue.submit(&[event]).map_err(Acquire::Register)
    }

    fn ignore() -> Result<mem::MaybeUninit<libc::sigaction>, Acquire> {
        // SAFETY: zero is a valid initial representation for sigaction on macOS.
        let mut action = unsafe { mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = libc::SIG_IGN;
        // SAFETY: sa_mask names writable storage; sigemptyset returns zero for it.
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        let mut previous = mem::MaybeUninit::<libc::sigaction>::uninit();
        // SAFETY: action is initialized and previous is writable. This final
        // acquisition call stores the old disposition only on success.
        unsafe {
            kernel::raw::Status::new(libc::sigaction(
                libc::SIGTERM,
                &action,
                previous.as_mut_ptr(),
            ))
            .try_zero::<Errno>(|| kernel::raw::Code::last::<Kqueue>().into())
        }
        .map_err(Acquire::Ignore)?;
        Ok(previous)
    }

    fn restore(previous: &mem::MaybeUninit<libc::sigaction>) -> Result<(), Errno> {
        // SAFETY: `previous` was initialized by a successful `ignore` call.
        unsafe {
            kernel::raw::Status::new(libc::sigaction(
                libc::SIGTERM,
                previous.as_ptr(),
                ptr::null_mut(),
            ))
            .try_zero::<Errno>(|| kernel::raw::Code::last::<Kqueue>().into())
        }
    }
}

impl fd::AsFd for Receiver {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl signal::raw::Receiver for Receiver {
    type Acquire = Acquire;
    type Wait = Wait;
    type Source<'a> = Source<'a>;

    unsafe fn try_new() -> Result<Self, Acquire> {
        let queue = Queue::new().map_err(Acquire::Kqueue)?;
        Self::register(&queue)?;
        let previous = Self::ignore()?;
        Ok(Self(queue, previous))
    }

    fn restore(&self) -> Result<(), signal::ReleaseFault> {
        // SAFETY: successful acquisition initialized previous while retaining
        // exclusive process signal authority; failure keeps this owner intact.
        Self::restore(&self.1).map_err(signal::ReleaseFault::Disposition)
    }

    fn source(&self) -> Self::Source<'_> {
        Source::new(self)
    }
}
