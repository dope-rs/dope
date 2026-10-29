use std::time;

use dope_core::{
    Errno,
    driver::{result, retained::flight::cancel, schedule::clock},
    io::{socket, transfer},
};
use o3::base::time::Positive;

use crate::link::oneshot::Cancellation;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Submit(result::SubmitError),
    Socket(socket::Create),
    Option(Errno),
    Connect(Errno),
    Send(Errno),
    SendAmount {
        observed: transfer::Len,
        submitted: transfer::NonZero,
    },
    SendDeadlineOverflow(clock::Range<Positive>),
    ConnectTimeout {
        late_by: time::Duration,
    },
    SendTimeout {
        late_by: time::Duration,
    },
    Cancelled,
    Cancellation((Cancellation, cancel::Failure)),
    Abandoned,
}

const _: () = assert!(
    std::mem::size_of::<Error>()
        == std::mem::size_of::<Result<clock::Range, (Cancellation, cancel::Failure)>>()
);

impl From<Cancellation> for Error {
    fn from(reason: Cancellation) -> Self {
        match reason {
            Cancellation::Cancelled => Self::Cancelled,
            Cancellation::ConnectTimeout { late_by } => Self::ConnectTimeout { late_by },
            Cancellation::SendTimeout { late_by } => Self::SendTimeout { late_by },
        }
    }
}
