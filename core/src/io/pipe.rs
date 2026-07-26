use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};

use crate::driver::Driver;
use crate::platform::raw::abi::PlatformAbi;

pub(crate) struct PipeEnds {
    read: OwnedFd,
    write: OwnedFd,
}

pub struct Pipe {
    ends: PipeEnds,
    _exclusive: PhantomData<Cell<()>>,
}

impl PipeEnds {
    pub(crate) fn new(read: OwnedFd, write: OwnedFd) -> Self {
        Self { read, write }
    }
}

impl Pipe {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            ends: Driver::open_pipe()?,
            _exclusive: PhantomData,
        })
    }

    pub fn write_end(&self) -> BorrowedFd<'_> {
        self.ends.write.as_fd()
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            ends: PipeEnds::new(self.ends.read.try_clone()?, self.ends.write.try_clone()?),
            _exclusive: PhantomData,
        })
    }

    pub fn read_end(&self) -> BorrowedFd<'_> {
        self.ends.read.as_fd()
    }

    pub fn notify(&self) -> io::Result<()> {
        let byte = 1u8;
        loop {
            let written =
                unsafe { libc::write(self.ends.write.as_raw_fd(), (&byte as *const u8).cast(), 1) };
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
