use std::{mem, time};

use dope_core::{
    Errno,
    driver::{life, result::poll, schedule::ready},
};
use drive::{clock, regress::Regression};
use poll::fault;

use crate::executor::drive;

/// Failure while driving one executor application.

#[derive(Debug)]
pub enum Error {
    Boot(life::Busy),
    Capacity(ready::Capacity),
    Poll(poll::Error),
    Completion(fault::Dispatch),
    Deadline(clock::Range),
    ClockMovedBackwards(Regression),
    TimerDisarm(Errno),
    Shutdown,
    QuiescenceTimeout,
    Timeout,
}

impl Error {
    pub(crate) const fn timeout(clock::Elapsed: clock::Elapsed) -> Self {
        Self::Timeout
    }

    pub(crate) const fn quiesce(clock::Elapsed: clock::Elapsed) -> Self {
        Self::QuiescenceTimeout
    }

    #[must_use]
    pub const fn kind(&self) -> drive::Kind<'_> {
        match self {
            Self::Boot(error) => drive::Kind::Boot(error),
            Self::Capacity(error) => drive::Kind::Capacity(error),
            Self::Poll(error) => drive::Kind::Poll(error),
            Self::Completion(fault) => drive::Kind::Completion(fault),
            Self::Deadline(range) => drive::Kind::Deadline(range),
            Self::ClockMovedBackwards(error) => drive::Kind::ClockMovedBackwards(error),
            Self::TimerDisarm(error) => drive::Kind::TimerDisarm(error),
            Self::Shutdown => drive::Kind::Shutdown,
            Self::QuiescenceTimeout => drive::Kind::QuiescenceTimeout,
            Self::Timeout => drive::Kind::Timeout,
        }
    }
}

impl Error {
    pub(crate) const fn regression(ask: time::Instant, current: time::Instant) -> Self {
        Self::ClockMovedBackwards(Regression::new(ask, current))
    }
}

impl From<life::Busy> for Error {
    fn from(error: life::Busy) -> Self {
        Self::Boot(error)
    }
}

impl From<ready::Capacity> for Error {
    fn from(error: ready::Capacity) -> Self {
        Self::Capacity(error)
    }
}

impl From<poll::Error> for Error {
    fn from(error: poll::Error) -> Self {
        Self::Poll(error)
    }
}

impl From<fault::Dispatch> for Error {
    fn from(failure: fault::Dispatch) -> Self {
        Self::Completion(failure)
    }
}

impl From<clock::Range> for Error {
    fn from(range: clock::Range) -> Self {
        Self::Deadline(range)
    }
}

const _: () = {
    assert!(!mem::needs_drop::<Error>());
    assert!(
        mem::size_of::<Error>() <= mem::size_of::<fault::Dispatch>() + mem::align_of::<Error>()
    );
};
