use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, RawFd};

pub(in crate::manifold::file) mod table;

use std::io::{Error, ErrorKind};
use std::process::abort;

use dope_core::backend::{Backend, RawSqe};
use dope_core::driver::token::Token;
use dope_core::io::file::OpenPath;
use dope_core::platform::Platform;

use super::source::Source;

type StatBuf = <Backend as Platform>::StatBuf;

pub(super) struct OpenRequest {
    path: OpenPath,
}

impl OpenRequest {
    pub(super) fn new(path: OpenPath) -> Self {
        Self { path }
    }

    pub(super) fn submission(&self, flags: i32, token: Token) -> RawSqe {
        self.path.open_at(flags, token)
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

    pub(super) fn submission(&mut self, token: Token) -> RawSqe {
        let output = self.output.as_mut_ptr();
        match &self.source {
            StatSource::Path(path) => RawSqe::stat_path(path.as_ptr(), output, token),
            StatSource::Fd(fd) => RawSqe::stat_fd(fd.as_raw_fd(), output, token),
        }
    }

    pub(super) fn complete(&mut self) -> StatBuf {
        unsafe { self.output.assume_init_read() }
    }

    pub(super) fn into_path(self) -> OpenPath {
        match self.source {
            StatSource::Path(path) => path,
            StatSource::Fd(_) => abort(),
        }
    }

    pub(super) fn into_source(self) -> Source<'d> {
        match self.source {
            StatSource::Fd(source) => source,
            StatSource::Path(_) => abort(),
        }
    }
}

pub(super) struct ReadRegion {
    buffer: Vec<u8>,
    len: u32,
}

impl ReadRegion {
    pub(super) fn new(buffer: Vec<u8>, len: u32) -> Result<Self, Vec<u8>> {
        if len == 0 || buffer.capacity() - buffer.len() < len as usize {
            return Err(buffer);
        }
        Ok(Self { buffer, len })
    }

    pub(super) fn submission(mut self, fd: RawFd, offset: u64, token: Token) -> (RawSqe, Self) {
        let sqe = RawSqe::read_raw(
            fd,
            self.buffer.spare_capacity_mut().as_mut_ptr().cast(),
            self.len as usize,
            offset,
            token,
        );
        (sqe, self)
    }

    pub(super) fn commit(mut self, amount: u32) -> Result<Vec<u8>, (Vec<u8>, Error)> {
        if amount > self.len {
            return Err((
                self.buffer,
                Error::new(
                    ErrorKind::InvalidData,
                    "dope::file: completion exceeded its prepared read region",
                ),
            ));
        }
        // SAFETY: this region exclusively retained the submitted allocation,
        // `amount` is bounded by its spare range, and a successful read
        // initialized those bytes.
        let initialized = self.buffer.len() + amount as usize;
        unsafe { self.buffer.set_len(initialized) };
        Ok(self.buffer)
    }

    pub(super) fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
}
