//! Opening outcomes for outbound connection slots.
use dope_core::driver::{result, route::Identity};

use crate::link::pool;

pub enum Attempt<'d, ID: Identity, P, R, E> {
    Submitted { key: pool::Key<'d, ID>, output: R },
    Deferred { cause: Deferred, input: P },
    Failed { error: Failure<E> },
}

/// A permanent failure before a socket becomes an outbound pool slot.
#[derive(Debug)]
pub enum Failure<E> {
    IdentityExhausted,
    Submission(result::SubmitError),
    Wire(E),
}

/// Permanent exhaustion of a connection or operation identity space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Exhaustion {
    Identity,
    Submission(result::SubmitError),
}

impl<E> From<Exhaustion> for Failure<E> {
    fn from(error: Exhaustion) -> Self {
        match error {
            Exhaustion::Identity => Self::IdentityExhausted,
            Exhaustion::Submission(error) => Self::Submission(error),
        }
    }
}

impl<E> Failure<E> {
    /// Separates a wire failure from the exact pool-level terminal cause.
    pub fn into_wire(self) -> Result<E, Exhaustion> {
        match self {
            Self::IdentityExhausted => Err(Exhaustion::Identity),
            Self::Submission(error) => Err(Exhaustion::Submission(error)),
            Self::Wire(cause) => Ok(cause),
        }
    }
}

/// A retryable reason an outbound socket open could not proceed.
#[derive(Clone, Copy)]
pub enum Deferred {
    Capacity,
    WireBackpressure,
    Submission(result::SubmitError),
}
