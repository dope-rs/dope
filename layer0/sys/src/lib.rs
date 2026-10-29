use std::{convert, io::Error, mem, num};

use o3::{base::time::Positive, buffer, mem::quota};

use crate::{io::fd, thread::timer};

pub mod engine;
pub mod io;
pub mod kernel;
pub mod memory;
pub mod ring;
pub mod table;

pub mod thread;

/// A positive operating-system error number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Errno(num::NonZero<i32>);

impl Errno {
    pub const BAD_DESCRIPTOR: Self = Self::fixed::<{ libc::EBADF }>();
    pub const CANCELED: Self = Self::fixed::<{ libc::ECANCELED }>();
    pub const IN_PROGRESS: Self = Self::fixed::<{ libc::EINPROGRESS }>();
    pub const NO_BUFFER_SPACE: Self = Self::fixed::<{ libc::ENOBUFS }>();
    pub const ALREADY_EXISTS: Self = Self::fixed::<{ libc::EEXIST }>();
    pub const PROTOCOL: Self = Self::fixed::<{ libc::EPROTO }>();
    pub const TOO_MANY_FILES: Self = Self::fixed::<{ libc::EMFILE }>();
    pub const INTERRUPTED: Self = Self::fixed::<{ libc::EINTR }>();
    pub const NOT_FOUND: Self = Self::fixed::<{ libc::ENOENT }>();
    pub const NOT_DIRECTORY: Self = Self::fixed::<{ libc::ENOTDIR }>();
    pub const SYMLINK_LOOP: Self = Self::fixed::<{ libc::ELOOP }>();
    pub const NAME_TOO_LONG: Self = Self::fixed::<{ libc::ENAMETOOLONG }>();
    pub const INVALID_ARGUMENT: Self = Self::fixed::<{ libc::EINVAL }>();

    const fn fixed<const CODE: i32>() -> Self {
        const { assert!(CODE > 0) };
        Self(
            num::NonZero::<u32>::MIN
                .saturating_add(CODE as u32 - 1)
                .cast_signed(),
        )
    }

    pub const fn code(self) -> num::NonZero<i32> {
        self.0
    }

    pub const fn is_intr(self) -> bool {
        self.0.get() == libc::EINTR
    }

    pub const fn is_canceled(self) -> bool {
        self.0.get() == libc::ECANCELED
    }

    pub const fn is_block(self) -> bool {
        self.0.get() == libc::EAGAIN || self.0.get() == libc::EWOULDBLOCK
    }

    pub const fn is_refused(self) -> bool {
        self.0.get() == libc::ECONNREFUSED
    }

    pub const fn is_pipe(self) -> bool {
        self.0.get() == libc::EPIPE
    }

    pub const fn is_not_found(self) -> bool {
        self.0.get() == libc::ENOENT || self.0.get() == libc::ENOTDIR
    }
}

impl TryFrom<i32> for Errno {
    type Error = kernel::Invalid;

    fn try_from(raw: i32) -> Result<Self, Self::Error> {
        match num::NonZero::new(raw) {
            Some(code) => Self::try_from(code),
            None => Err(kernel::Invalid {
                actual: 0,
                minimum: 1,
                maximum: i32::MAX as libc::ssize_t,
            }),
        }
    }
}

impl TryFrom<num::NonZero<i32>> for Errno {
    type Error = kernel::Invalid;

    fn try_from(code: num::NonZero<i32>) -> Result<Self, Self::Error> {
        if code.get() > 0 {
            Ok(Self(code))
        } else {
            Err(kernel::Invalid {
                actual: code.get() as libc::ssize_t,
                minimum: 1,
                maximum: i32::MAX as libc::ssize_t,
            })
        }
    }
}

impl TryFrom<Error> for Errno {
    type Error = Error;

    fn try_from(error: Self::Error) -> Result<Self, Self::Error> {
        match error.raw_os_error() {
            Some(raw) => Self::try_from(raw).map_err(|_| error),
            None => Err(error),
        }
    }
}

impl From<Errno> for Error {
    fn from(error: Errno) -> Self {
        Self::from_raw_os_error(error.code().get())
    }
}

const _: () = {
    assert!(mem::size_of::<Errno>() == mem::size_of::<i32>());
    assert!(mem::size_of::<Option<Errno>>() == mem::size_of::<i32>());
    assert!(mem::size_of::<Result<(), Errno>>() == mem::size_of::<i32>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Cloexec {
    Get(Errno),
    Set(Errno),
}

impl fd::Flags for Cloexec {
    const READ: libc::c_int = libc::F_GETFD;
    const WRITE: libc::c_int = libc::F_SETFD;
    const BIT: libc::c_int = libc::FD_CLOEXEC;
}

impl From<fd::Failure> for Cloexec {
    fn from(error: fd::Failure) -> Self {
        match error {
            fd::Failure::Get(error) => Self::Get(error),
            fd::Failure::Set(error) => Self::Set(error),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Cloexec>() == 8);
    assert!(mem::align_of::<Cloexec>() == mem::align_of::<Errno>());
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Nonblock {
    Get(Errno),
    Set(Errno),
}

impl fd::Flags for Nonblock {
    const READ: libc::c_int = libc::F_GETFL;
    const WRITE: libc::c_int = libc::F_SETFL;
    const BIT: libc::c_int = libc::O_NONBLOCK;
}

impl From<fd::Failure> for Nonblock {
    fn from(error: fd::Failure) -> Self {
        match error {
            fd::Failure::Get(error) => Self::Get(error),
            fd::Failure::Set(error) => Self::Set(error),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Nonblock>() == 8);
    assert!(mem::align_of::<Nonblock>() == mem::align_of::<Errno>());
};

/// Creation and operations on a live kernel deadline.
pub trait Timer: Sized {
    fn try_new(deadline: Positive) -> Result<Self, timer::Error>;
    fn try_refresh(&self) -> Result<(), Errno>;
    fn try_disarm(&self) -> Result<(), Errno>;
}

impl Timer for convert::Infallible {
    fn try_new(deadline: Positive) -> Result<Self, timer::Error> {
        Err(timer::Error::Unsupported { deadline })
    }

    fn try_refresh(&self) -> Result<(), Errno> {
        match *self {}
    }

    fn try_disarm(&self) -> Result<(), Errno> {
        match *self {}
    }
}

pub mod probe;

/// A native receive lease retained until its exact completed prefix is released.
pub trait Buffer: Sized {
    fn output(&mut self) -> buffer::raw::Output<'_>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn recycle(self);
}

/// A borrowed scheduler budget; unsuccessful acquisition spends no quota.
pub trait Budget {
    fn remaining(&self) -> usize;
    fn admit<T>(&mut self, acquire: impl FnOnce() -> Option<T>) -> quota::Admission<T>;
}
