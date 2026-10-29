use std::os::fd;

/// Creates two exclusively owned native pipe endpoints.
pub trait Pipe {
    fn open() -> Result<(fd::OwnedFd, fd::OwnedFd), Error>;
    fn nonblocking() -> Result<(fd::OwnedFd, fd::OwnedFd), Error>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum End {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Open(crate::Errno),
    CloseOnExec { end: End, failure: crate::Cloexec },
    Nonblocking { end: End, failure: crate::Nonblock },
}
