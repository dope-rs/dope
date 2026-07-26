use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;

use super::source::Source;
use dope_core::backend::Backend;
use dope_core::backend::Sqe;
use dope_core::driver::token::Token;
use dope_core::io::file::OpenPath;
use dope_core::platform::Platform;

type StatBuf = <Backend as Platform>::StatBuf;

pub(super) struct OpenRequest {
    path: OpenPath,
}

impl OpenRequest {
    pub(super) fn new(path: OpenPath) -> Self {
        Self { path }
    }

    pub(super) fn submission(&self, flags: i32, token: Token) -> Sqe {
        unsafe { self.path.open_at(flags, token) }
    }
}

enum StatSource<'d> {
    Path(OpenPath),
    Fd(Source<'d>),
}

pub(super) struct StatRequest<'d> {
    source: StatSource<'d>,
    output: MaybeUninit<StatBuf>,
}

impl<'d> StatRequest<'d> {
    pub(super) fn path(path: OpenPath) -> Self {
        Self {
            source: StatSource::Path(path),
            output: MaybeUninit::zeroed(),
        }
    }

    pub(super) fn fd(source: Source<'d>) -> Self {
        Self {
            source: StatSource::Fd(source),
            output: MaybeUninit::zeroed(),
        }
    }

    pub(super) fn submission(&mut self, token: Token) -> Sqe {
        let output = self.output.as_mut_ptr();
        match &self.source {
            StatSource::Path(path) => Sqe::stat_path(path.as_ptr(), output, token),
            StatSource::Fd(fd) => Sqe::stat_fd(fd.as_raw_fd(), output, token),
        }
    }

    pub(super) fn complete(&mut self) -> StatBuf {
        unsafe { self.output.assume_init_read() }
    }
}

pub(super) struct ReadRegion {
    ptr: NonNull<MaybeUninit<u8>>,
    len: u32,
}

impl ReadRegion {
    pub(super) fn new(buffer: &mut Vec<u8>) -> Option<Self> {
        let len = buffer.len().min(u32::MAX as usize) as u32;
        (len != 0).then(|| Self {
            ptr: unsafe { NonNull::new_unchecked(buffer.as_mut_ptr().cast()) },
            len,
        })
    }

    pub(super) fn submission(self, fd: RawFd, offset: u64, token: Token) -> (Sqe, Self) {
        let span = unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) };
        let sqe = unsafe { Sqe::read_uninit(fd, span, offset, token) };
        (sqe, self)
    }

    pub(super) fn commit(self, buffer: &mut Vec<u8>, amount: u32) -> io::Result<()> {
        if amount > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: completion exceeded its prepared read region",
            ));
        }
        if buffer.as_mut_ptr().cast() != self.ptr.as_ptr() || buffer.len() < self.len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dope::file: prepared read buffer changed before completion",
            ));
        }
        Ok(())
    }
}
