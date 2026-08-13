use std::{io, marker, mem, os::fd, path, rc};

use crate::{
    backend,
    driver::route::{self, kind},
    io::transfer,
    platform,
};

pub(crate) mod raw;

pub struct RawMetadata {
    pub len: u64,
    pub modified: Option<(i64, u32)>,
    pub regular: bool,
}

pub struct Directory {
    inner: rc::Rc<fd::OwnedFd>,
    _thread: o3::ThreadBound,
}

impl Directory {
    pub fn open(path: impl AsRef<path::Path>) -> io::Result<Self> {
        let directory = <backend::Backend as platform::Filesystem>::open_directory(path.as_ref())?;
        Ok(Self {
            inner: rc::Rc::new(directory),
            _thread: o3::ThreadBound::NEW,
        })
    }

    pub fn relative(&self, path: &str) -> io::Result<OpenPath> {
        validate_relative(path)?;
        let path = c_path(path)?;
        Ok(OpenPath {
            path,
            directory: rc::Rc::clone(&self.inner),
        })
    }
}

pub struct OpenPath {
    pub(crate) path: std::ffi::CString,
    pub(crate) directory: rc::Rc<fd::OwnedFd>,
}

pub struct Native(marker::PhantomData<fn()>);

/// The durability boundary requested for a file sync operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sync {
    /// Persist file data and the metadata required to retrieve it.
    Data,
    /// Persist all file data and metadata.
    All,
}

#[allow(private_bounds)]
pub trait Mode: raw::Mode {}

impl<F: raw::Mode> Mode for F {}

#[repr(transparent)]
pub struct Metadata<F: Mode> {
    raw: <F as raw::Mode>::Metadata,
    mode: marker::PhantomData<fn() -> F>,
}

/// A native file operation borrowing every descriptor and output it captures.
///
/// Raw tokens cannot enter the safe file submission boundary:
///
/// ```compile_fail
/// use std::{mem::MaybeUninit, os::fd::BorrowedFd};
/// use dope_core::{
///     driver::route::{KeyTag, Token},
///     io::fs::{Native, Submission},
/// };
///
/// fn submit_raw(fd: BorrowedFd<'_>, buffer: &mut [MaybeUninit<u8>], token: Token) {
///     let _ = Submission::<Native, KeyTag<1>>::read_uninit(fd, buffer, 0, token);
/// }
/// ```
///
/// ```compile_fail
/// use std::{mem::MaybeUninit, os::fd::AsFd};
/// use dope_core::{
///     driver::{Reference, route::{Epoch, KeyTag, SlotIndex}},
///     io::fs::{Native, Submission},
/// };
///
/// fn free_buffer_while_borrowed<'d>(driver: Reference<'d>) {
///     let target = driver.targets::<KeyTag<1>>().bind(SlotIndex::ZERO, Epoch::INITIAL);
///     let file = std::fs::File::open("data").unwrap();
///     let mut buffer = vec![MaybeUninit::<u8>::uninit(); 8];
///     let submission = Submission::<Native, KeyTag<1>>::read_uninit(
///         file.as_fd(), &mut buffer, 0, target,
///     ).unwrap();
///     drop(buffer);
///     drop(submission);
/// }
/// ```
pub struct Submission<'a, 'd, F, Tag>
where
    F: Mode,
    Tag: route::Tag,
{
    pub(in crate::io::fs) raw: <F as raw::Mode>::Raw,
    pub(in crate::io::fs) target: route::Operation<'d, Tag>,
    borrow: marker::PhantomData<&'a mut ()>,
    mode: marker::PhantomData<fn() -> F>,
}

pub(crate) struct OpenState<F: Mode> {
    path: OpenPath,
    beneath: <F as raw::Mode>::Beneath,
    mode: marker::PhantomData<fn() -> F>,
}

impl<F: Mode> Metadata<F> {
    #[doc(hidden)]
    pub fn parse(self) -> io::Result<RawMetadata> {
        let metadata = <F as raw::Mode>::parse(&self.raw)?;
        if metadata
            .modified
            .is_some_and(|(_, nanos)| nanos >= 1_000_000_000)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope: stat returned an invalid modification nanosecond",
            ));
        }
        Ok(metadata)
    }
}

impl<'a, 'd, F, Tag> Submission<'a, 'd, F, Tag>
where
    F: Mode,
    Tag: route::Tag,
{
    fn bind(raw: <F as raw::Mode>::Raw, target: route::Operation<'d, Tag>) -> Self {
        Self {
            raw,
            target,
            borrow: marker::PhantomData,
            mode: marker::PhantomData,
        }
    }

    pub fn read_uninit(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a mut [mem::MaybeUninit<u8>],
        offset: u64,
        target: route::Target<'d, Tag>,
    ) -> io::Result<Self> {
        let len = transfer::Len::try_io(buffer.len())?;
        Ok(Self::bind(
            <F as raw::Mode>::read(fd, buffer, len, offset),
            target.operation(kind::READ),
        ))
    }

    pub fn read(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a mut [u8],
        offset: u64,
        target: route::Target<'d, Tag>,
    ) -> io::Result<Self> {
        let len = transfer::Len::try_io(buffer.len())?;
        Ok(Self::bind(
            <F as raw::Mode>::read_initialized(fd, buffer, len, offset),
            target.operation(kind::READ),
        ))
    }

    #[doc(hidden)]
    pub fn write(
        fd: fd::BorrowedFd<'a>,
        buffer: &'a [u8],
        offset: u64,
        target: route::Target<'d, Tag>,
    ) -> io::Result<Self> {
        let len = transfer::Len::try_io(buffer.len())?;
        Ok(Self::bind(
            <F as raw::Mode>::write(fd, buffer, len, offset),
            target.operation(kind::WRITE),
        ))
    }

    pub fn stat_fd(
        fd: fd::BorrowedFd<'a>,
        output: &'a mut mem::MaybeUninit<Metadata<F>>,
        target: route::Target<'d, Tag>,
    ) -> Self {
        Self::bind(
            <F as raw::Mode>::stat(fd, output),
            target.operation(kind::STAT),
        )
    }

    /// Submit a durability barrier for all writes completed before this operation.
    pub fn sync(fd: fd::BorrowedFd<'a>, mode: Sync, target: route::Target<'d, Tag>) -> Self {
        Self::bind(
            <F as raw::Mode>::sync(fd, mode),
            target.operation(kind::SYNC),
        )
    }
}

impl<F: Mode> OpenState<F> {
    pub(crate) fn new(path: OpenPath) -> Self {
        Self {
            path,
            beneath: <F as raw::Mode>::confined_regular_open(),
            mode: marker::PhantomData,
        }
    }

    pub(crate) fn submission<'d, Tag: route::Tag>(
        &self,
        target: route::Target<'d, Tag>,
    ) -> Submission<'_, 'd, F, Tag> {
        Submission::bind(
            <F as raw::Mode>::open(
                &self.beneath,
                fd::AsFd::as_fd(self.path.directory.as_ref()),
                self.path.path.as_c_str(),
            ),
            target.operation(kind::OPEN),
        )
    }
}

#[doc(hidden)]
pub struct OpenRequest<F: Mode> {
    state: OpenState<F>,
}

impl<F: Mode> OpenRequest<F> {
    pub fn submission<'d, Tag: route::Tag>(
        &self,
        target: route::Target<'d, Tag>,
    ) -> Submission<'_, 'd, F, Tag> {
        self.state.submission(target)
    }
}

impl OpenPath {
    #[doc(hidden)]
    pub fn regular_request<F: Mode>(self) -> OpenRequest<F> {
        OpenRequest {
            state: OpenState::new(self),
        }
    }
}

fn c_path(path: &str) -> io::Result<std::ffi::CString> {
    std::ffi::CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "dope: path has interior nul"))
}

fn validate_relative(path: &str) -> io::Result<()> {
    const COMPONENT_MAX: usize = 255;

    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "dope: path is not a confined relative path",
        )
    };

    if path.is_empty() || path.starts_with('/') {
        return Err(invalid());
    }

    let mut meaningful = false;
    let mut oversized_component = false;
    for component in path.split('/') {
        if component == ".." {
            return Err(invalid());
        }
        meaningful |= !component.is_empty() && component != ".";
        oversized_component |= component.len() > COMPONENT_MAX;
    }

    if !meaningful {
        return Err(invalid());
    }
    if path.len() >= libc::PATH_MAX as usize || oversized_component {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dope: confined path exceeds the platform limit",
        ));
    }
    Ok(())
}
