use std::os::fd::{AsRawFd as _, BorrowedFd, IntoRawFd as _, OwnedFd};

use crate::{
    Errno,
    io::fd,
    kernel::{self, errno},
};

pub mod raw;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Get(Errno),
    Set(Errno),
}

pub trait Flags: From<Failure> + Sized {
    const READ: libc::c_int;
    const WRITE: libc::c_int;
    const BIT: libc::c_int;
}

/// Native descriptor operations using the calling thread's exact error slot.
pub trait Fd: errno::Source {
    /// Adds a descriptor flag, retrying only each interrupted native attempt.
    fn update<E: Flags>(file: BorrowedFd<'_>) -> Result<(), E> {
        update::<Self, E, true, true>(file)
    }

    /// Removes one descriptor flag, preserving all other native flags.
    fn remove<E: Flags>(file: BorrowedFd<'_>) -> Result<(), E> {
        update::<Self, E, true, false>(file)
    }

    /// Adds a descriptor flag with one get attempt followed by one set attempt.
    fn try_update<E: Flags>(file: BorrowedFd<'_>) -> Result<(), E> {
        update::<Self, E, false, true>(file)
    }

    /// Consumes descriptor ownership through one native close attempt.
    /// The returned errno is observable; ownership is never issued for a retry.
    fn close<E: From<Errno>>(file: OwnedFd) -> Result<(), E> {
        // SAFETY: ownership is transferred exactly once to this close attempt.
        let raw = unsafe { libc::close(file.into_raw_fd()) };
        // SAFETY: close returns zero or -1; no intervening syscall changes errno.
        unsafe {
            kernel::raw::Status::new(raw).try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn duplicate<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<OwnedFd, E> {
        // SAFETY: the descriptor is live; F_DUPFD_CLOEXEC creates a new owned descriptor.
        let raw = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        // SAFETY: a successful result is the fresh descriptor returned above.
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn set<const COMMAND: libc::c_int, E: From<Errno>>(
        file: BorrowedFd<'_>,
        flags: libc::c_int,
    ) -> Result<(), E> {
        set::<Self, E, COMMAND, false>(file, flags, E::from)
    }

    /// Applies scalar descriptor flags, retrying only interrupted attempts.
    fn set_retry<const COMMAND: libc::c_int, E: From<Errno>>(
        file: BorrowedFd<'_>,
        flags: libc::c_int,
    ) -> Result<(), E> {
        set::<Self, E, COMMAND, true>(file, flags, E::from)
    }
    fn read<'a, E: From<Errno>>(
        file: BorrowedFd<'_>,
        output: &'a mut [u8],
    ) -> Result<&'a mut [u8], E> {
        // SAFETY: output exclusively owns the writable extent until read returns.
        let raw = unsafe { libc::read(file.as_raw_fd(), output.as_mut_ptr().cast(), output.len()) };
        // SAFETY: read returns -1 or initializes at most this exact output extent.
        unsafe {
            crate::io::raw::Buffer::try_native(output, raw, || {
                kernel::raw::Code::last::<Self>().into()
            })
        }
    }

    fn write<'a, E: From<Errno>>(file: BorrowedFd<'_>, input: &'a [u8]) -> Result<&'a [u8], E> {
        // SAFETY: input retains the complete readable extent until write returns.
        let raw = unsafe { libc::write(file.as_raw_fd(), input.as_ptr().cast(), input.len()) };
        // SAFETY: write returns -1 or a count no larger than this exact input extent.
        unsafe {
            crate::io::raw::Buffer::try_native(input, raw, || {
                kernel::raw::Code::last::<Self>().into()
            })
        }
    }
}

impl<T: errno::Source> Fd for T {}

fn update<S: Fd + ?Sized, E: Flags, const RETRY: bool, const ENABLED: bool>(
    file: BorrowedFd<'_>,
) -> Result<(), E> {
    const {
        assert!(
            (E::READ == libc::F_GETFD && E::WRITE == libc::F_SETFD)
                || (E::READ == libc::F_GETFL && E::WRITE == libc::F_SETFL)
        );
    }
    // SAFETY: READ admits only a live descriptor's scalar get command; errno
    // is captured immediately after each native failure on the same thread.
    let flags = unsafe {
        kernel::raw::try_retry::<_, E, RETRY>(
            || {
                kernel::raw::Status::new(libc::fcntl(file.as_raw_fd(), E::READ))
                    .try_value::<Errno>(|| kernel::raw::Code::last::<S>().into())
            },
            |error| E::from(Failure::Get(error)),
        )
    }?
    .get();
    let flags = if ENABLED {
        flags | E::BIT
    } else {
        flags & !E::BIT
    };
    if E::WRITE == libc::F_SETFD {
        set::<S, E, { libc::F_SETFD }, RETRY>(file, flags, |error| E::from(Failure::Set(error)))
    } else {
        set::<S, E, { libc::F_SETFL }, RETRY>(file, flags, |error| E::from(Failure::Set(error)))
    }
}

fn set<S: Fd + ?Sized, E, const COMMAND: libc::c_int, const RETRY: bool>(
    file: BorrowedFd<'_>,
    flags: libc::c_int,
    classify: impl FnOnce(Errno) -> E,
) -> Result<(), E> {
    const { assert!(COMMAND == libc::F_SETFD || COMMAND == libc::F_SETFL) };
    // SAFETY: COMMAND admits only scalar flag updates on this borrowed
    // descriptor; errno is captured immediately after each native failure.
    unsafe {
        kernel::raw::try_retry::<_, E, RETRY>(
            || {
                kernel::raw::Status::new(libc::fcntl(file.as_raw_fd(), COMMAND, flags))
                    .try_zero::<Errno>(|| kernel::raw::Code::last::<S>().into())
            },
            classify,
        )
    }
}
