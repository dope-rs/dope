use std::{cell, marker, mem, os::fd, time};

use dope_sys::io::{
    fd::Fd as _,
    pipe,
    poll::{self, Poll as _},
};

use crate::{Backend, Errno, platform::Flags};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateError {
    Open(Errno),
    CloseOnExec { end: pipe::End, failure: Flags },
    Nonblocking { end: pipe::End, failure: Flags },
    Fork(Errno),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WaitError {
    Read(Errno),
    Record {
        expected: usize,
        actual: isize,
    },
    Poll(Errno),
    Timeout {
        timeout: time::Duration,
        elapsed: time::Duration,
    },
    PollResult {
        result: i32,
        revents: i16,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NotifyError {
    Write(Errno),
    Record { expected: usize, actual: isize },
}

/// An exclusively used wake reader whose ownership may transfer during startup.
#[repr(transparent)]
pub struct Wait(fd::OwnedFd, marker::PhantomData<cell::Cell<()>>);

#[repr(transparent)]
#[derive(Debug)]
pub struct Notify(fd::OwnedFd, marker::PhantomData<cell::Cell<()>>);

pub struct Ends<R> {
    read: R,
    notify: Notify,
}

impl Ends<Wait> {
    pub fn blocking() -> Result<Self, CreateError> {
        let (read, write) = <Backend as pipe::Pipe>::open().map_err(CreateError::from_native)?;
        Ok(Self {
            read: Wait(read, marker::PhantomData),
            notify: Notify(write, marker::PhantomData),
        })
    }
}

impl Ends<fd::OwnedFd> {
    pub fn event() -> Result<Self, CreateError> {
        let (read, write) =
            <Backend as pipe::Pipe>::nonblocking().map_err(CreateError::from_native)?;
        Ok(Self {
            read,
            notify: Notify(write, marker::PhantomData),
        })
    }
}

impl<R> Ends<R> {
    pub fn split(self) -> (R, Notify) {
        (self.read, self.notify)
    }
}

impl Wait {
    pub fn wait(&self) -> Result<(), WaitError> {
        let mut byte = [0u8];
        loop {
            match Backend::read::<Errno>(fd::AsFd::as_fd(&self.0), &mut byte) {
                Ok([_]) => return Ok(()),
                Ok(bytes) => {
                    return Err(WaitError::Record {
                        expected: 1,
                        actual: bytes.len() as isize,
                    });
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(WaitError::Read(error)),
            }
        }
    }

    pub fn wait_timeout(&self, timeout: time::Duration) -> Result<(), WaitError> {
        let started = time::Instant::now();
        let total = timeout.as_nanos();
        let mut delay = total.saturating_sub(started.elapsed().as_nanos());
        loop {
            let nanos = delay.min(i32::MAX as u128 * 1_000_000) as u64;
            let millis = nanos.div_ceil(1_000_000) as i32;
            let mut entries = [poll::Entry::readable(fd::AsFd::as_fd(&self.0))];
            match Backend::wait::<_, Errno>(&mut entries, millis) {
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(WaitError::Poll(error)),
                Ok(events) => {
                    let result = events.count() as i32;
                    let entry = events.entries()[0];
                    match (result, entry.revents()) {
                        (1, _) if entry.is_readable() => return self.wait(),
                        (0, 0) => {}
                        (_, revents) => return Err(WaitError::PollResult { result, revents }),
                    }
                }
            }
            let elapsed = started.elapsed();
            delay = total.saturating_sub(elapsed.as_nanos());
            if delay == 0 {
                return Err(WaitError::Timeout { timeout, elapsed });
            }
        }
    }
}

impl Notify {
    pub fn notify(&self) -> Result<(), NotifyError> {
        loop {
            match Backend::write::<Errno>(fd::AsFd::as_fd(&self.0), &[1]) {
                Ok([_]) => return Ok(()),
                Ok(bytes) => {
                    return Err(NotifyError::Record {
                        expected: 1,
                        actual: bytes.len() as isize,
                    });
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(NotifyError::Write(error)),
            }
        }
    }

    pub fn fork(self) -> Result<(Self, Self), CreateError> {
        let guard =
            Backend::duplicate::<Errno>(fd::AsFd::as_fd(&self.0)).map_err(CreateError::Fork)?;
        Ok((Self(guard, marker::PhantomData), self))
    }
}

const _: () = {
    assert!(mem::size_of::<Wait>() == mem::size_of::<fd::OwnedFd>());
    assert!(mem::align_of::<Wait>() == mem::align_of::<fd::OwnedFd>());
    assert!(mem::size_of::<Notify>() == mem::size_of::<fd::OwnedFd>());
    assert!(mem::align_of::<Notify>() == mem::align_of::<fd::OwnedFd>());
};

impl CreateError {
    fn from_native(error: pipe::Error) -> Self {
        match error {
            pipe::Error::Open(error) => Self::Open(error),
            pipe::Error::CloseOnExec { end, failure } => Self::CloseOnExec {
                end,
                failure: Flags::cloexec(failure),
            },
            pipe::Error::Nonblocking { end, failure } => Self::Nonblocking {
                end,
                failure: Flags::nonblock(failure),
            },
        }
    }
}

impl fd::AsFd for Wait {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        fd::AsFd::as_fd(&self.0)
    }
}
impl fd::AsFd for Notify {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        fd::AsFd::as_fd(&self.0)
    }
}
