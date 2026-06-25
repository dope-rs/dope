use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, RawFd};

use crate::backend::sqe::Sqe;
use crate::backend::token::Token;

pub struct OsFile {
    inner: File,
}

impl OsFile {
    pub fn create(path: &str) -> io::Result<Self> {
        let inner = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { inner })
    }

    pub fn open(path: &str) -> io::Result<Self> {
        let inner = File::options().read(true).open(path)?;
        Ok(Self { inner })
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.inner.metadata()?.len())
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

pub struct OpenPath {
    path: CString,
}

impl OpenPath {
    pub fn new(path: &str) -> io::Result<Self> {
        let path = CString::new(path).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "dope: path has interior nul")
        })?;
        Ok(Self { path })
    }

    pub fn as_ptr(&self) -> *const libc::c_char {
        self.path.as_ptr()
    }
}

pub const O_RDONLY: i32 = libc::O_RDONLY;
pub const O_CLOEXEC: i32 = libc::O_CLOEXEC;

pub fn open_at(path: &OpenPath, flags: i32, op: Token) -> Sqe {
    Sqe::openat(libc::AT_FDCWD, path.as_ptr(), flags, 0, op)
}

pub fn read_at(file: &OsFile, buf: &mut [u8], offset: u64, op: Token) -> Sqe {
    Sqe::read(file.fd(), buf, offset, op)
}

pub fn read_fd(fd: RawFd, buf: &mut [u8], offset: u64, op: Token) -> Sqe {
    Sqe::read(fd, buf, offset, op)
}

pub fn splice_file_to_pipe(
    file: &OsFile,
    off_in: i64,
    pipe_write_fd: RawFd,
    len: u32,
    op: Token,
) -> Sqe {
    Sqe::splice_raw(file.fd(), off_in, pipe_write_fd, -1, len, 0, op)
}

pub fn splice_fd_to_pipe(
    fd_in: RawFd,
    off_in: i64,
    pipe_write_fd: RawFd,
    len: u32,
    op: Token,
) -> Sqe {
    Sqe::splice_raw(fd_in, off_in, pipe_write_fd, -1, len, 0, op)
}
