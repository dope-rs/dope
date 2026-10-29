use std::{fs, mem, os::fd};

use dope_sys::io::{fd::Fd as _, file, request::Request};
use file::File as _;
use o3::{base::marker::ThreadBound, buffer::bytes};

use crate::{
    Backend, Errno,
    driver::{
        route,
        route::{
            kind,
            kind::{Class, Open, Write},
        },
    },
    io::{fs::metadata::Metadata, transfer},
};

mod dir;

mod len;
pub mod metadata;
pub mod open;
pub mod path;

pub use dir::Dir;
pub use len::Len;

/// Native file kind observed from descriptor metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Kind(file::Kind);

impl Kind {
    pub(crate) const fn from_meta(raw: file::Metadata) -> Self {
        Self(raw.kind())
    }

    pub const fn is_regular(self) -> bool {
        self.0.is_regular()
    }
}

/// Absolute byte position representable by every file backend.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Offset(file::Offset);

/// Exact position or length outside the absolute file-offset domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Overflow {
    pub actual: u64,
    pub maximum: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Extent {
    offset: Offset,
    length: transfer::Len,
}

impl From<Len> for Offset {
    fn from(length: Len) -> Self {
        Self(length.0)
    }
}

impl From<transfer::Len> for Offset {
    fn from(length: transfer::Len) -> Self {
        Self(file::Offset::clamp(length.into_raw() as u64))
    }
}

impl Offset {
    pub const fn fixed<const OFFSET: u64>() -> Self {
        const { assert!(OFFSET <= i64::MAX as u64) };
        Self(file::Offset::clamp(OFFSET))
    }

    pub const fn try_new(value: u64) -> Result<Self, Overflow> {
        match file::Offset::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(Overflow {
                actual: value,
                maximum: i64::MAX as u64,
            }),
        }
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn checked_add(self, length: transfer::Len) -> Result<Self, Extent> {
        let value = self.get() + length.into_raw() as u64;
        match Self::try_new(value) {
            Ok(value) => Ok(value),
            Err(_) => Err(Extent::new(self, length)),
        }
    }

    pub const fn checked_sub(self, length: transfer::Len) -> Result<Self, Extent> {
        match self.0.checked_sub(length.into_raw() as u64) {
            Some(value) => Ok(Self(value)),
            None => Err(Extent::new(self, length)),
        }
    }
}

impl Extent {
    const fn new(offset: Offset, length: transfer::Len) -> Self {
        Self { offset, length }
    }
}

const _: () = {
    assert!(mem::size_of::<Kind>() == mem::size_of::<file::Kind>());
    assert!(mem::align_of::<Kind>() == mem::align_of::<file::Kind>());
    assert!(mem::size_of::<Offset>() == mem::size_of::<u64>());
    assert!(mem::align_of::<Offset>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Extent>() == 2 * mem::size_of::<u64>());
    assert!(mem::align_of::<Extent>() == mem::align_of::<u64>());
    assert!(mem::size_of::<Result<Offset, Extent>>() == 3 * mem::size_of::<u64>());
    assert!(mem::align_of::<Result<Offset, Extent>>() == mem::align_of::<u64>());
};

/// A native file operation borrowing every descriptor and output it captures.
///
/// Raw tokens cannot enter the safe file submission boundary:
///
///
///
/// Keeping the captured buffer until the submission is dropped is valid.
pub struct Submission<'a, 'd, Tag, C = <Tag as route::Tag>::Class>
where
    Tag: route::Tag,
    C: Class,
{
    raw: <Backend as file::File>::Request<'a>,
    target: route::Operation<'d, Tag, C>,
}

pub struct OpenRequest {
    path: path::Confined,
    beneath: <Backend as file::File>::Options,
}

impl<'a, 'd, Tag, C> Submission<'a, 'd, Tag, C>
where
    Tag: route::Tag,
    C: Class,
{
    fn bind(
        raw: <Backend as file::File>::Request<'a>,
        target: route::Operation<'d, Tag, C>,
    ) -> Self {
        Self { raw, target }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        <Backend as file::File>::Request<'a>,
        route::Operation<'d, Tag, C>,
    ) {
        (self.raw, self.target)
    }
}

impl<'a, 'd, Tag> Submission<'a, 'd, Tag, Write>
where
    Tag: route::Tag,
{
    #[doc(hidden)]
    pub fn write_prefix(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a [u8],
        offset: Offset,
        target: route::Target<'d, Tag>,
    ) -> Self
    where
        Tag: route::Select<Write>,
    {
        let input = bytes::Prefix::new(buffer);
        Self::bind(
            Request::write(fd, input, offset.0),
            target.operation::<Write>(),
        )
    }
}

impl<'a, 'd, Tag> Submission<'a, 'd, Tag, kind::Sync>
where
    Tag: route::Tag,
{
    /// Submit a durability barrier for all writes completed before this operation.
    pub fn sync(fd: fd::BorrowedFd<'a>, mode: Sync, target: route::Target<'d, Tag>) -> Self
    where
        Tag: route::Select<kind::Sync>,
    {
        Self::bind(
            Request::sync(
                fd,
                match mode {
                    Sync::Data => file::Sync::Data,
                    Sync::All => file::Sync::All,
                },
            ),
            target.operation::<kind::Sync>(),
        )
    }

    /// Atomically replaces `new` with `old` within one retained directory.
    #[doc(hidden)]
    pub fn rename(rename: path::Rename<'a>, target: route::Target<'d, Tag>) -> Self
    where
        Tag: route::Select<kind::Sync>,
    {
        Self::bind(
            Request::rename(
                fd::AsFd::as_fd(rename.directory()),
                rename.old(),
                rename.new_name(),
            ),
            target.operation::<kind::Sync>(),
        )
    }

    /// Removes one retained directory entry.
    #[doc(hidden)]
    pub fn unlink(entry: path::Entry<'a>, target: route::Target<'d, Tag>) -> Self
    where
        Tag: route::Select<kind::Sync>,
    {
        Self::bind(
            Request::unlink(fd::AsFd::as_fd(entry.directory()), entry.name()),
            target.operation::<kind::Sync>(),
        )
    }
}

impl OpenRequest {
    /// Keeps the confined path non-following and close-on-exec for every opening policy.
    pub fn new(path: path::Confined, options: open::Options) -> Self {
        let (flags, mode) = options.nofollow().lower();
        Self {
            path,
            beneath: <<Backend as file::File>::Request<'_> as Request>::options(flags, mode),
        }
    }

    pub fn submission<'d, Tag>(
        &self,
        target: route::Target<'d, Tag>,
    ) -> Submission<'_, 'd, Tag, Open>
    where
        Tag: route::Tag + route::Select<Open>,
    {
        let path = self.path.view();
        Submission::bind(
            Request::open(
                fd::AsFd::as_fd(path.directory()),
                path.name(),
                &self.beneath,
            ),
            target.operation::<Open>(),
        )
    }

    pub fn into_path(self) -> path::Confined {
        self.path
    }
}

/// The durability boundary requested for a submitted file sync.
pub enum Sync {
    Data,
    All,
}

/// One owned file descriptor, including its kernel file offset and locks.
#[repr(transparent)]
pub struct File {
    fd: fd::OwnedFd,
    thread: ThreadBound,
}

impl File {
    pub fn try_read<'a>(&self, bytes: &'a mut [u8]) -> Result<&'a mut [u8], Errno> {
        Backend::read::<dope_sys::Errno>(fd::AsFd::as_fd(self), bytes)
    }

    /// Reads one prefix without changing this file's current position.
    pub fn try_read_at<'a>(
        &self,
        bytes: &'a mut [u8],
        offset: Offset,
    ) -> Result<&'a mut [u8], Errno> {
        Backend::read_at::<dope_sys::Errno>(fd::AsFd::as_fd(self), bytes, offset.0)
    }

    pub fn try_write<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], Errno> {
        Backend::write::<dope_sys::Errno>(fd::AsFd::as_fd(self), bytes)
    }

    pub fn metadata(&self) -> Result<Metadata, Errno> {
        let value = Backend::metadata::<dope_sys::Errno>(fd::AsFd::as_fd(self))?;
        Ok(Metadata {
            kind: Kind::from_meta(value),
            length: value.size(),
        })
    }

    /// Attempts an exclusive advisory lock without waiting for its current owner.
    pub fn try_lock(&self) -> Result<(), Errno> {
        Backend::try_lock::<dope_sys::Errno>(fd::AsFd::as_fd(self))
    }

    pub fn try_sync(&self) -> Result<(), Errno> {
        Backend::sync::<dope_sys::Errno>(fd::AsFd::as_fd(self))
    }
}

impl fd::AsFd for File {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl From<fd::OwnedFd> for File {
    fn from(file: fd::OwnedFd) -> Self {
        Self {
            fd: file,
            thread: ThreadBound::NEW,
        }
    }
}

impl From<fs::File> for File {
    fn from(file: fs::File) -> Self {
        Self {
            fd: file.into(),
            thread: ThreadBound::NEW,
        }
    }
}

impl From<File> for fs::File {
    fn from(file: File) -> Self {
        file.fd.into()
    }
}

impl From<File> for fd::OwnedFd {
    fn from(file: File) -> Self {
        file.fd
    }
}

const _: () = {
    assert!(size_of::<File>() == size_of::<fd::OwnedFd>());
    assert!(align_of::<File>() == align_of::<fd::OwnedFd>());
};
