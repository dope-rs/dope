use dope_net::link::egress::{
    self, load,
    reserve::{self, error},
};

use crate::network::listener::conn::write;

/// One observed value at an ordered reservation position.
pub enum Part<B> {
    Empty,
    Prefix(egress::Charge),
    Body(load::Value<B>),
}

/// Exact reason an observed bounded draft cannot own final-link capacity.
#[derive(Debug)]
pub enum Bounded {
    Payload(load::Error),
    Wire(egress::WireError),
    Geometry(error::Full),
}

/// Exact draft rejection after its physical owners have been released.
#[derive(Debug)]
pub enum Cause {
    Payload(load::Error),
    Wire(egress::WireError),
    Geometry(error::Full),
    Values { cause: error::Cause },
}

/// Rejected values retaining every caller-owned value and terminal intent.
pub enum Mismatch<B, R> {
    Bounded {
        cause: Bounded,
        commit: write::Commit<(), load::Value<B>, R>,
    },
    Values {
        cause: error::Cause,
        expected: reserve::Bounds,
        first: Part<B>,
        second: Part<B>,
        receipt: R,
        end: write::End,
    },
}

/// Recoverable failure of one listener write attempt.
pub enum Error<B, R, E> {
    Capacity(write::Reject),
    Build(E),
    Mismatch(Mismatch<B, R>),
}

impl<B, R> Mismatch<B, R> {
    /// Releases rejected payload owners and returns their exact cause and receipt.
    /// The failed bound is carried by the cause; the full reservation plan is released.
    pub fn into_parts(self) -> (Cause, R) {
        match self {
            Self::Bounded { cause, commit } => {
                let (_, _, receipt, _) = commit.into_parts();
                let cause = match cause {
                    Bounded::Payload(cause) => Cause::Payload(cause),
                    Bounded::Wire(cause) => Cause::Wire(cause),
                    Bounded::Geometry(cause) => Cause::Geometry(cause),
                };
                (cause, receipt)
            }
            Self::Values { cause, receipt, .. } => (Cause::Values { cause }, receipt),
        }
    }

    pub(in crate::network::listener) const fn values(
        cause: error::Cause,
        expected: reserve::Bounds,
        first: Part<B>,
        second: Part<B>,
        receipt: R,
        end: write::End,
    ) -> Self {
        Self::Values {
            cause,
            expected,
            first,
            second,
            receipt,
            end,
        }
    }
}
