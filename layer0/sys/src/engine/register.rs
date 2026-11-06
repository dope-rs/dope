use crate::{Errno, io::reactor::Full};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Interest {
    Readable,
    Closed,
}

#[derive(Clone, Copy, Debug)]
pub struct Source<'a> {
    pub fd: std::os::fd::BorrowedFd<'a>,
    pub interest: Interest,
}

/// Exact failure while arming the engine's retained shutdown source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Full(Full),
    Register(Errno),
}
