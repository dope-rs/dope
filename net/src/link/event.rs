use std::{error, fmt, io};

use dope_core::{driver, io::socket};

use crate::{link::pool, wire::send};

/// Terminal metadata for one send submission.
pub struct SendCompletion<'d, const ID: u8> {
    key: pool::Key<'d, ID>,
    sent: send::Sent,
}

impl<'d, const ID: u8> SendCompletion<'d, ID> {
    pub(in crate::link) fn new(key: pool::Key<'d, ID>, sent: send::Sent) -> Self {
        Self { key, sent }
    }

    pub fn sent(&self) -> send::Sent {
        self.sent
    }

    pub fn key(&self) -> pool::Key<'d, ID> {
        self.key
    }
}

#[derive(Debug)]
pub enum ConnectFailure {
    Socket(io::Error),
    Admission(driver::SubmitError),
    Connect(io::Error),
    NoTarget,
}

impl ConnectFailure {
    pub fn into_io_error(self) -> io::Error {
        match self {
            Self::Socket(error) | Self::Connect(error) => error,
            Self::Admission(error) => error.into(),
            Self::NoTarget => io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: connector attempt has no transport target",
            ),
        }
    }
}

impl fmt::Display for ConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(error) => write!(formatter, "socket creation/tuning failed: {error}"),
            Self::Admission(error) => write!(formatter, "connect submission failed: {error}"),
            Self::Connect(error) => write!(formatter, "connect failed: {error}"),
            Self::NoTarget => formatter.write_str("connector attempt has no transport target"),
        }
    }
}

impl error::Error for ConnectFailure {}

pub enum Socket<'d, const ID: u8, X> {
    Pending,
    Failed {
        key: pool::Key<'d, ID>,
        attempt: X,
        cause: ConnectFailure,
    },
    Stale,
}

pub enum Connect<'d, const ID: u8, X> {
    Connected {
        key: pool::Key<'d, ID>,
        attempt: X,
        peer: socket::Addr,
    },
    Failed {
        key: pool::Key<'d, ID>,
        attempt: X,
        cause: ConnectFailure,
    },
    Stale,
}

pub enum DispatchRecv<'d, const ID: u8, C> {
    Drop,
    Close(pool::Key<'d, ID>),
    Overrun(pool::Key<'d, ID>),
    Chunk(pool::Key<'d, ID>, C),
    NoChunk(pool::Key<'d, ID>),
    Discarded(pool::Key<'d, ID>),
}

pub enum DataReservation<'d, const ID: u8, P, C> {
    Ready { prepared: P, completion: C },
    Parked(ParkRecv<'d, ID>),
    Drop,
}

pub enum ControlDispatch<'d, const ID: u8, C> {
    Ready(DispatchRecv<'d, ID, C>),
    Parked(ParkRecv<'d, ID>),
}

pub enum ParkRecv<'d, const ID: u8> {
    Deferred,
    Close(pool::Key<'d, ID>),
}

pub enum SendOutcome<'d, const ID: u8> {
    Sent(SendCompletion<'d, ID>),
    Close(SendCompletion<'d, ID>),
    Drop,
}
