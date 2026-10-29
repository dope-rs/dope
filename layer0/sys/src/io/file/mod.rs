use std::{
    mem,
    os::fd::{AsRawFd as _, BorrowedFd, OwnedFd},
};

use o3::{
    alloc,
    base::num::bounded::{U32, U64},
    buffer::bytes::{self, bounded},
};
pub use opened::Opened;
pub use sync::Sync;

use crate::{
    Errno,
    io::{fd, request::Request},
    kernel::{self, errno},
};

mod kind;
mod metadata;
mod mode;
mod open;
mod opened;
pub mod raw;
mod sync;

pub use kind::Kind;
pub use metadata::Metadata;
pub use mode::Mode;
pub use open::Open;

/// An absolute file position, excluding native current-position sentinels.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Offset(U64<0, { i64::MAX as u64 }>);

impl Offset {
    pub const fn new(value: u64) -> Option<Self> {
        match U64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn clamp(value: u64) -> Self {
        Self(U64::clamp(value))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Subtracts a byte count while retaining the admitted absolute range.
    pub const fn checked_sub(self, value: u64) -> Option<Self> {
        match self.0.checked_sub(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Transfers ownership from a native file completion exactly once.
pub trait Completion {
    type Opened: raw::Completion;
}

/// A native directory stream whose descriptor and buffered entries have one owner.
#[repr(transparent)]
pub struct Dir<S: raw::Directory>(S::Stream);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenError<E> {
    Syscall(E),
    Allocation(alloc::Exhausted),
}

impl<S: File> Dir<S> {
    pub fn open<E: From<Errno>>(path: &std::ffi::CStr) -> Result<Self, OpenError<E>> {
        S::open_dir(path).map(Self)
    }

    pub fn from_fd<E: From<Errno>>(file: OwnedFd) -> Result<Self, OpenError<E>> {
        S::from_fd(file).map(Self)
    }

    pub fn entries(&mut self) -> (BorrowedFd<'_>, Entries<'_, S>) {
        let file = S::descriptor(&self.0);
        (
            // SAFETY: Directory preserves this descriptor until the stream is dropped,
            // and its mutable borrow excludes dropping the owner for both returned borrows.
            unsafe { BorrowedFd::borrow_raw(file.get()) },
            Entries(&mut self.0),
        )
    }
}

#[repr(transparent)]
pub struct Entries<'a, S: raw::Directory>(&'a mut S::Stream);

/// One name and the file kind already supplied by its native directory record.
#[derive(Clone, Copy, Debug)]
pub struct Entry<'a> {
    pub name: &'a std::ffi::CStr,
    pub kind: Option<Kind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError<E> {
    Syscall(E),
    Invalid(kernel::Invalid),
}

impl<S: raw::Directory> Entries<'_, S> {
    /// The next read requires the previous name's borrow to end.
    pub fn try_next<E: From<Errno>>(&mut self) -> Result<Option<Entry<'_>>, ReadError<E>> {
        S::next(self.0)
    }
}

/// Selects native file requests while preserving every caller-owned borrow.
pub trait File: errno::Source + raw::Directory {
    type Options;
    type Request<'a>: Request<'a, Options = Self::Options, Buffer = bytes::Prefix<'a, { bounded::MAX }>>
    where
        Self: 'a;
    fn open<E: From<Errno>>(path: &std::ffi::CStr, flags: Open, mode: Mode) -> Result<OwnedFd, E> {
        let raw = unsafe { libc::open(path.as_ptr(), flags.raw(), mode.raw() as libc::c_uint) };
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn metadata<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<Metadata, E> {
        let mut value = mem::MaybeUninit::uninit();
        let raw = unsafe { libc::fstat(file.as_raw_fd(), value.as_mut_ptr()) };
        // SAFETY: fstat returns zero after filling value or -1 on failure.
        unsafe {
            kernel::raw::Status::new(raw).try_zero::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?;
        Ok(Metadata::new(unsafe { value.assume_init() }))
    }

    fn stat<E: From<Errno>>(path: &std::ffi::CStr) -> Result<Metadata, E> {
        let mut value = mem::MaybeUninit::uninit();
        let raw = unsafe { libc::stat(path.as_ptr(), value.as_mut_ptr()) };
        // SAFETY: stat returns zero after filling value or -1 on failure.
        unsafe {
            kernel::raw::Status::new(raw).try_zero::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?;
        Ok(Metadata::new(unsafe { value.assume_init() }))
    }

    fn mkdir<E: From<Errno>>(path: &std::ffi::CStr, mode: Mode) -> Result<(), E> {
        // SAFETY: the terminated path stays borrowed; mkdir returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::mkdir(path.as_ptr(), mode.raw()))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Classifies the named entry itself while retaining its parent directory.
    fn metadata_at<E: From<Errno>>(
        dir: Option<BorrowedFd<'_>>,
        name: &std::ffi::CStr,
    ) -> Result<Metadata, E> {
        let mut value = mem::MaybeUninit::uninit();
        // SAFETY: the parent, name and complete native metadata output stay live.
        let raw = unsafe {
            libc::fstatat(
                dir.map_or(libc::AT_FDCWD, |dir| dir.as_raw_fd()),
                name.as_ptr(),
                value.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        // SAFETY: fstatat returns zero with initialized metadata or -1 with errno.
        unsafe {
            kernel::raw::Status::new(raw).try_zero::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?;
        Ok(Metadata::new(unsafe { value.assume_init() }))
    }

    fn unlink<E: From<Errno>>(path: &std::ffi::CStr) -> Result<(), E> {
        // SAFETY: path is terminated and retained; unlink returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::unlink(path.as_ptr()))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn rename<E: From<Errno>>(from: &std::ffi::CStr, to: &std::ffi::CStr) -> Result<(), E> {
        // SAFETY: both terminated paths remain borrowed; rename returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::rename(from.as_ptr(), to.as_ptr()))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Attempts an exclusive advisory lock without waiting for its current owner.
    fn try_lock<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<(), E> {
        // SAFETY: the descriptor remains borrowed; nonblocking flock returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn sync<E: From<Errno>>(file: BorrowedFd<'_>) -> Result<(), E> {
        // SAFETY: the descriptor remains borrowed; fsync returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::fsync(file.as_raw_fd()))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn truncate<E: From<Errno>>(file: BorrowedFd<'_>, len: i64) -> Result<(), E> {
        // SAFETY: the descriptor remains borrowed; ftruncate returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::ftruncate(file.as_raw_fd(), len))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn unlink_at<E: From<Errno>>(dir: BorrowedFd<'_>, name: &std::ffi::CStr) -> Result<(), E> {
        // SAFETY: the descriptor and terminated name remain borrowed; unlinkat returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0))
                .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Removes one empty directory entry without following a replacement symlink.
    fn remove_dir<E: From<Errno>>(
        dir: Option<BorrowedFd<'_>>,
        name: &std::ffi::CStr,
    ) -> Result<(), E> {
        // SAFETY: the parent and terminated name stay borrowed; unlinkat returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::unlinkat(
                dir.map_or(libc::AT_FDCWD, |dir| dir.as_raw_fd()),
                name.as_ptr(),
                libc::AT_REMOVEDIR,
            ))
            .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    fn open_at<E: From<Errno>>(
        dir: BorrowedFd<'_>,
        name: &std::ffi::CStr,
        flags: Open,
        mode: Mode,
    ) -> Result<OwnedFd, E> {
        let raw = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr(),
                flags.raw(),
                mode.raw() as libc::c_uint,
            )
        };
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(raw, || kernel::raw::Code::last::<Self>().into())
        }
    }

    fn rename_at<E: From<Errno>>(
        dir: BorrowedFd<'_>,
        old: &std::ffi::CStr,
        new: &std::ffi::CStr,
    ) -> Result<(), E> {
        // SAFETY: the descriptor and terminated names remain borrowed; renameat returns zero or -1.
        unsafe {
            kernel::raw::Status::new(libc::renameat(
                dir.as_raw_fd(),
                old.as_ptr(),
                dir.as_raw_fd(),
                new.as_ptr(),
            ))
            .try_zero(|| kernel::raw::Code::last::<Self>().into())
        }
    }

    /// Reads one prefix without changing the descriptor's current position.
    fn read_at<'a, E: From<Errno>>(
        file: BorrowedFd<'_>,
        output: &'a mut [u8],
        offset: Offset,
    ) -> Result<&'a mut [u8], E> {
        const { assert!(libc::off_t::MAX as u64 == i64::MAX as u64) };
        // SAFETY: the descriptor stays borrowed, output exclusively owns its
        // writable extent, and the admitted nonnegative offset fits the ABI.
        let raw = unsafe {
            libc::pread(
                file.as_raw_fd(),
                output.as_mut_ptr().cast(),
                output.len(),
                offset.get() as libc::off_t,
            )
        };
        // SAFETY: pread returns -1 or initializes at most this exact output
        // extent; failure reads the unchanged errno on this thread.
        unsafe {
            crate::io::raw::Buffer::try_native(output, raw, || {
                kernel::raw::Code::last::<Self>().into()
            })
        }
    }

    fn write_at<E: From<Errno>>(
        file: BorrowedFd<'_>,
        input: bytes::Prefix<'_, { bounded::MAX }>,
        offset: Offset,
    ) -> Result<U32<0, { bounded::MAX as u32 }>, E> {
        use kernel::raw::Status;

        const { assert!(libc::off_t::MAX as u64 == i64::MAX as u64) };
        let input = input.into_slice();
        // SAFETY: the offset's admitted nonnegative range fits the native ABI.
        let raw = unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                input.as_ptr().cast(),
                input.len(),
                offset.get() as libc::off_t,
            )
        };
        // SAFETY: pwrite returns -1 or a count within this Prefix's i32::MAX-bound
        // input; failure reads the unchanged errno on this thread.
        Ok(unsafe {
            Status::from_count(raw).try_count::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?
        .bounded())
    }
}
