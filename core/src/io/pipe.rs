use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

use crate::driver::Driver;
use crate::platform::raw::abi::PlatformAbi;

pub struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
    _exclusive: PhantomData<Cell<()>>,
}

impl Pipe {
    pub fn new() -> io::Result<Self> {
        let fds = Driver::open_pipe()?;
        Ok(Self::from_fds(fds[0], fds[1]))
    }

    pub fn write_end(&self) -> BorrowedFd<'_> {
        self.write.as_fd()
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            read: self.read.try_clone()?,
            write: self.write.try_clone()?,
            _exclusive: PhantomData,
        })
    }

    pub fn from_fds(read: RawFd, write: RawFd) -> Self {
        unsafe {
            Self {
                read: OwnedFd::from_raw_fd(read),
                write: OwnedFd::from_raw_fd(write),
                _exclusive: PhantomData,
            }
        }
    }

    pub fn read_end(&self) -> BorrowedFd<'_> {
        self.read.as_fd()
    }

    pub fn notify(&self) -> io::Result<()> {
        let byte = 1u8;
        loop {
            let written =
                unsafe { libc::write(self.write.as_raw_fd(), (&byte as *const u8).cast(), 1) };
            if written == 1 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(error),
            }
        }
    }
}
