//! Opening outcomes for outbound connection slots.

use std::{error, fmt};

use crate::link::pool;

pub enum Outcome<'d, const ID: u8, P, R> {
    Submitted { key: pool::Key<'d, ID>, output: R },
    Deferred { cause: Deferred, input: P },
}

#[must_use = "the rejected input still owns its terminal-settlement state"]
pub struct Rejected<P, E> {
    input: P,
    failure: Failure<E>,
}

impl<P, E> Rejected<P, E> {
    pub(in crate::link::pool) const fn new(input: P, failure: Failure<E>) -> Self {
        Self { input, failure }
    }

    pub fn into_parts(self) -> (P, Failure<E>) {
        (self.input, self.failure)
    }
}

/// A permanent failure before a socket becomes an outbound pool slot.
#[derive(Debug)]
pub enum Failure<E> {
    Wire(E),
}

/// A retryable reason an outbound socket open could not proceed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Deferred {
    Capacity,
    WireBackpressure,
    SubmissionBackpressure,
}

impl<E: fmt::Display> fmt::Display for Failure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => write!(formatter, "wire open failed: {error}"),
        }
    }
}

impl<E: error::Error + 'static> error::Error for Failure<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Wire(error) => Some(error),
        }
    }
}
