use std::{mem, os::fd};

use dope_sys::io::{
    fd::Fd as _,
    file::{self, File as _, Mode, Open},
    request,
};
use o3::{
    base::marker::ThreadBound,
    buffer::bytes::{self, bounded::MAX},
};

use crate::{Kqueue, file::Options, raw};

/// A file request retaining its exact descriptor and buffer borrows.
#[repr(transparent)]
pub struct Request<'a>(State<'a>, ThreadBound);

impl<'a> Request<'a> {
    pub fn watch(
        watch: &'a crate::watch::Watch,
        _: bytes::bounded::Value<&'a mut [mem::MaybeUninit<u8>]>,
    ) -> Self {
        match *watch {}
    }
}

enum State<'a> {
    Open {
        dir: fd::BorrowedFd<'a>,
        path: &'a std::ffi::CStr,
        options: &'a Options,
    },
    Metadata {
        file: fd::BorrowedFd<'a>,
        output: &'a mut raw::Metadata,
    },
    Read {
        file: fd::BorrowedFd<'a>,
        output: bytes::bounded::Value<&'a mut [mem::MaybeUninit<u8>]>,
        offset: file::Offset,
    },
    Write {
        file: fd::BorrowedFd<'a>,
        input: bytes::Prefix<'a, MAX>,
        offset: file::Offset,
    },
    WriteCurrent {
        file: fd::BorrowedFd<'a>,
        input: bytes::Prefix<'a, MAX>,
    },
    Sync {
        file: fd::BorrowedFd<'a>,
        mode: file::Sync,
    },
    Rename {
        dir: fd::BorrowedFd<'a>,
        old: &'a std::ffi::CStr,
        new: &'a std::ffi::CStr,
    },
    Unlink {
        dir: fd::BorrowedFd<'a>,
        name: &'a std::ffi::CStr,
    },
}

impl<'a> request::Request<'a> for Request<'a> {
    type Buffer = bytes::Prefix<'a, MAX>;
    type Options = Options;
    type Metadata = raw::Metadata;

    fn options(flags: Open, mode: Mode) -> Self::Options {
        Options { flags, mode }
    }

    fn open(dir: fd::BorrowedFd<'a>, path: &'a std::ffi::CStr, options: &'a Self::Options) -> Self {
        Self(State::Open { dir, path, options }, ThreadBound::NEW)
    }

    fn metadata(file: fd::BorrowedFd<'a>, output: &'a mut Self::Metadata) -> Self {
        Self(State::Metadata { file, output }, ThreadBound::NEW)
    }

    fn write(file: fd::BorrowedFd<'a>, input: Self::Buffer, offset: file::Offset) -> Self {
        Self(
            State::Write {
                file,
                input,
                offset,
            },
            ThreadBound::NEW,
        )
    }

    fn read(
        file: fd::BorrowedFd<'a>,
        output: bytes::bounded::Value<&'a mut [mem::MaybeUninit<u8>]>,
        offset: file::Offset,
    ) -> Self {
        Self(
            State::Read {
                file,
                output,
                offset,
            },
            ThreadBound::NEW,
        )
    }

    fn write_current(file: fd::BorrowedFd<'a>, input: Self::Buffer) -> Self {
        Self(State::WriteCurrent { file, input }, ThreadBound::NEW)
    }

    fn sync(file: fd::BorrowedFd<'a>, mode: file::Sync) -> Self {
        Self(State::Sync { file, mode }, ThreadBound::NEW)
    }

    fn rename(dir: fd::BorrowedFd<'a>, old: &'a std::ffi::CStr, new: &'a std::ffi::CStr) -> Self {
        Self(State::Rename { dir, old, new }, ThreadBound::NEW)
    }

    fn unlink(dir: fd::BorrowedFd<'a>, name: &'a std::ffi::CStr) -> Self {
        Self(State::Unlink { dir, name }, ThreadBound::NEW)
    }
}

impl request::Execute for Request<'_> {
    fn execute<C: request::Complete>(self, done: C) -> C::Output {
        let result = match self.0 {
            State::Open { dir, path, options } => {
                return done.opened(Kqueue::open_at(dir, path, options.flags, options.mode));
            }
            State::Metadata { file, output } => {
                Kqueue::metadata::<dope_sys::Errno>(file).map(|value| output.write(value))
            }
            State::Read {
                file,
                output,
                offset,
            } => {
                let result = Kqueue::read_uninit_at(file, output.into_inner(), offset)
                    .map(|read| bytes::Prefix::<MAX>::new(read).len());
                return done.read(result);
            }
            State::Write {
                file,
                input,
                offset,
            } => {
                let result = Kqueue::write_at(file, input, offset);
                return done.write(result);
            }
            State::WriteCurrent { file, input } => {
                let result = Kqueue::write(file, input.as_slice())
                    .map(|written| bytes::Prefix::<MAX>::new(written).len());
                return done.write(result);
            }
            State::Sync { file, mode } => super::sync(file, mode),
            State::Rename { dir, old, new } => Kqueue::rename_at(dir, old, new),
            State::Unlink { dir, name } => Kqueue::unlink_at(dir, name),
        };
        done.sync(result)
    }
}

const _: () = {
    assert!(mem::size_of::<Request<'_>>() == 5 * mem::size_of::<usize>());
    assert!(mem::align_of::<Request<'_>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Request<'_>>>() == mem::size_of::<Request<'_>>());
};
