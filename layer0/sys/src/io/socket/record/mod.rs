use crate::Errno;

mod packet;
mod stream;

pub use packet::Packet;
pub use stream::Stream;

/// A local transport carrying fixed u32 records through borrowed native prefixes.
pub trait Transport {
    /// Sends one record. Each successful leaf returns its completed input prefix.
    fn send(
        value: u32,
        send: impl for<'a> FnMut(&'a [u8]) -> Result<&'a [u8], Errno>,
    ) -> Result<(), Fault>;

    /// Receives one record. Each successful leaf returns its initialized output prefix.
    fn recv(recv: impl for<'a> FnMut(&'a mut [u8]) -> Result<&'a mut [u8], Errno>) -> Record;
}

#[derive(Debug)]
pub struct Size {
    pub expected: usize,
    /// Observed payload extent, capped at five bytes for an oversized record.
    pub actual: usize,
}

#[derive(Debug)]
pub enum Fault {
    Syscall(Errno),
    Record(Size),
}

#[derive(Debug)]
pub enum Record {
    Value(u32),
    Closed,
    Failed(Fault),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum End {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Open(Errno),
    Cloexec { end: End, error: crate::Cloexec },
    Sigpipe { end: End, error: Errno },
}
