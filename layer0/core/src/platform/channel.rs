use std::{
    cell, marker,
    os::fd::{self, AsFd as _},
};

use dope_sys::io::socket::{
    How, Socket,
    record::{self, Fault, Record, Transport as _},
};

use crate::{Backend, Errno, platform::Flags};

/// One exclusively used endpoint of a reliable local channel carrying u32 records.
/// Ownership may transfer during startup; shared cross-thread access is excluded.
#[derive(Debug)]
pub struct Channel(fd::OwnedFd, marker::PhantomData<cell::Cell<()>>);

impl Channel {
    pub fn pair() -> Result<(Self, Self), Create> {
        let (left, right) = Backend::pair().map_err(Create::from_native)?;
        Ok((
            Self(left, marker::PhantomData),
            Self(right, marker::PhantomData),
        ))
    }

    /// A failed send is terminal: a stream may already have transferred a prefix.
    pub fn send(&self, value: u32) -> Result<(), Fault> {
        <Backend as Socket>::Records::send(value, |bytes| {
            Backend::send_quiet::<Errno>(self.0.as_fd(), bytes)
        })
    }

    /// Receives one exact record, retaining native packet or stream framing.
    /// A syscall error is terminal: a stream may already have transferred a prefix.
    pub fn recv(&self) -> Record {
        <Backend as Socket>::Records::recv(|bytes| Backend::recv::<0, Errno>(self.0.as_fd(), bytes))
    }

    pub fn shutdown(&self) -> Result<(), Errno> {
        Backend::shutdown::<Errno>(self.0.as_fd(), How::WRITE)
    }
}

impl fd::AsFd for Channel {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Create {
    Open(Errno),
    Cloexec { end: record::End, error: Flags },
    Sigpipe { end: record::End, error: Errno },
}

impl Create {
    fn from_native(error: record::Error) -> Self {
        match error {
            record::Error::Open(error) => Self::Open(error),
            record::Error::Cloexec { end, error } => Self::Cloexec {
                end,
                error: Flags::cloexec(error),
            },
            record::Error::Sigpipe { end, error } => Self::Sigpipe { end, error },
        }
    }
}
