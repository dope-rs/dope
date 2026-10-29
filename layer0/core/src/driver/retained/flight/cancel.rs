use std::convert::Infallible;

use dope_sys::engine::admit;

use crate::driver::{
    result,
    result::{poll, poll::fault},
    retained::flight,
};

type NativeCause = <crate::Backend as admit::Native>::Error<poll::Cause>;

pub enum Outcome {
    Submitted,
    Queued,
    Completed,
    Deferred,
}

/// Exact rejection of the selected flight before native cancellation commitment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure<C = NativeCause> {
    echo: fault::Echo,
    cause: C,
}

impl<C> Failure<C> {
    pub const fn echo(&self) -> fault::Echo {
        self.echo
    }
    pub fn into_parts(self) -> (fault::Echo, Cause)
    where
        Cause: From<C>,
    {
        (self.echo, Cause::from(self.cause))
    }
}

/// Capacity and native-state failures retain different recovery requirements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<C = NativeCause> {
    Full(result::SubmitError),
    Rejected(Failure<C>),
}

impl<C> Error<C> {
    pub(crate) fn new(echo: flight::raw::Echo, error: admit::Error<C>) -> Self {
        match error {
            admit::Error::Full(full) => Self::Full(result::SubmitError::queue(full)),
            admit::Error::Rejected(cause) => Self::Rejected(Failure {
                echo: fault::Echo::new(echo),
                cause,
            }),
        }
    }
}

/// Exact rejected native state, projected only when the failure is consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    Absent,
    Reactor(poll::Cause),
}

impl From<Infallible> for Cause {
    fn from(cause: Infallible) -> Self {
        match cause {}
    }
}

impl From<admit::Cause<poll::Cause>> for Cause {
    fn from(cause: admit::Cause<poll::Cause>) -> Self {
        match cause {
            admit::Cause::Absent(admit::Absent) => Self::Absent,
            admit::Cause::Reactor(cause) => Self::Reactor(cause),
        }
    }
}
