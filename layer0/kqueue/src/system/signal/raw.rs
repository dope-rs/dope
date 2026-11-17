use std::{mem, os::fd, ptr};

use dope_sys::{Errno, io::reactor, kernel, thread::signal};

use crate::{
    Kqueue,
    system::{
        Queue,
        signal::{Acquire, Source, Wait},
    },
};

#[derive(Debug)]
pub struct Receiver {
    queue: Queue,
    terminate: mem::MaybeUninit<libc::sigaction>,
    interrupt: Option<mem::MaybeUninit<libc::sigaction>>,
}

impl Receiver {
    fn register(queue: &crate::system::Queue) -> Result<(), Acquire> {
        let terminate = libc::kevent {
            ident: libc::SIGTERM as libc::uintptr_t,
            filter: libc::EVFILT_SIGNAL,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        };
        let interrupt = libc::kevent {
            ident: libc::SIGINT as libc::uintptr_t,
            ..terminate
        };
        queue
            .submit(&[terminate, interrupt])
            .map_err(Acquire::Register)
    }

    fn ignore(signal: libc::c_int) -> Result<mem::MaybeUninit<libc::sigaction>, Acquire> {
        // SAFETY: zero is a valid initial representation for sigaction on macOS.
        let mut action = unsafe { mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = libc::SIG_IGN;
        // SAFETY: sa_mask names writable storage; sigemptyset returns zero for it.
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        let mut previous = mem::MaybeUninit::<libc::sigaction>::uninit();
        // SAFETY: the signal is SIGTERM or SIGINT; action is initialized and
        // previous is writable. Failure leaves this disposition unchanged.
        unsafe {
            kernel::raw::Status::new(libc::sigaction(
                signal,
                &action,
                previous.as_mut_ptr(),
            ))
            .try_zero::<Kqueue, Errno>()
        }
        .map_err(Acquire::Ignore)?;
        Ok(previous)
    }

    fn restore(
        signal: libc::c_int,
        previous: &mem::MaybeUninit<libc::sigaction>,
    ) -> Result<(), Errno> {
        // SAFETY: `previous` was initialized by a successful `ignore` call.
        unsafe {
            kernel::raw::Status::new(libc::sigaction(
                signal,
                previous.as_ptr(),
                ptr::null_mut(),
            ))
            .try_zero::<Kqueue, Errno>()
        }
    }
}

impl fd::AsFd for Receiver {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.queue.as_fd()
    }
}

impl signal::raw::Receiver for Receiver {
    type Wait = Wait;
    type Source<'a> = Source<'a>;

    unsafe fn try_new() -> Result<Self, signal::raw::Acquire<Self>> {
        let queue = Queue::new().map_err(|error| match error {
            reactor::Create::Open(error) => Acquire::QueueOpen(error),
            reactor::Create::Cloexec(error) => Acquire::QueueCloexec(error),
        })?;
        Self::register(&queue)?;
        let terminate = Self::ignore(libc::SIGTERM)?;
        let mut receiver = Self {
            queue,
            terminate,
            interrupt: None,
        };
        match Self::ignore(libc::SIGINT) {
            Ok(previous) => {
                receiver.interrupt = Some(previous);
                Ok(receiver)
            }
            Err(cause) => Err(signal::raw::Acquire::Retained { cause, receiver }),
        }
    }

    fn restore(&self) -> Result<(), signal::ReleaseFault> {
        // SAFETY: successful acquisition initialized previous while retaining
        // exclusive process signal authority; failure keeps this owner intact.
        if let Some(previous) = &self.interrupt {
            Self::restore(libc::SIGINT, previous).map_err(signal::ReleaseFault::Disposition)?;
        }
        Self::restore(libc::SIGTERM, &self.terminate)
            .map_err(signal::ReleaseFault::Disposition)
    }

    fn source(&self) -> Self::Source<'_> {
        Source::new(self)
    }
}
