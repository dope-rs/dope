use o3::marker::ThreadBound;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Error, ErrorKind};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::backend::RawSqe;
use crate::driver::token::Token;
use libc::AT_FDCWD;
use libc::c_char;

pub struct RawMetadata {
    pub len: u64,
    pub modified: Option<(i64, u32)>,
    pub regular: bool,
}

pub struct OsFile {
    inner: File,
    _thread: ThreadBound,
}

impl OsFile {
    pub fn create(path: &str) -> io::Result<Self> {
        let inner = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            inner,
            _thread: ThreadBound::NEW,
        })
    }

    pub fn open(path: &str) -> io::Result<Self> {
        let inner = File::options().read(true).open(path)?;
        Ok(Self {
            inner,
            _thread: ThreadBound::NEW,
        })
    }

    pub fn try_len(&self) -> io::Result<u64> {
        Ok(self.inner.metadata()?.len())
    }

    pub fn try_is_empty(&self) -> io::Result<bool> {
        Ok(self.try_len()? == 0)
    }

    pub fn fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

pub struct OpenPath {
    path: CString,
}

impl OpenPath {
    pub fn new(path: &str) -> io::Result<Self> {
        let path = CString::new(path)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "dope: path has interior nul"))?;
        Ok(Self { path })
    }

    pub fn as_ptr(&self) -> *const c_char {
        self.path.as_ptr()
    }

    pub fn open_at(&self, flags: i32, op: Token) -> RawSqe {
        RawSqe::openat(AT_FDCWD, self.path.as_ptr(), flags, 0, op)
    }
}

pub const O_RDONLY: i32 = libc::O_RDONLY;
pub const O_CLOEXEC: i32 = libc::O_CLOEXEC;
