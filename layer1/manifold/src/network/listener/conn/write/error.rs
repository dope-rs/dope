use dope_net::link::egress::{
    self, load,
    reserve::{self, error},
};

use crate::network::listener::conn::write;

#[derive(Debug)]
pub enum Admission {
    Deferred(write::Deferred),
    Closed,
    Rejected(write::Reject),
}

/// One observed value at an ordered reservation position.
pub enum Part<B> {
    Empty,
    Prefix(egress::Charge),
    Body(load::Value<B>),
}

#[derive(Debug)]
pub enum Bounded {
    Payload(load::Error),
    Wire(egress::WireError),
}

#[derive(Debug)]
pub enum Cause {
    Payload(load::Error),
    Wire(egress::WireError),
    Values { cause: error::Cause },
}

/// Rejected publication retaining the original body and terminal intent.
pub enum Mismatch<B> {
    Bounded {
        cause: Bounded,
        written: usize,
        body: Option<load::Value<B>>,
        end: write::End,
    },
    Values {
        cause: error::Cause,
        expected: reserve::Bounds,
        first: Part<B>,
        second: Part<B>,
        end: write::End,
    },
}

impl<B> Mismatch<B> {
    pub fn into_cause(self) -> Cause {
        match self {
            Self::Bounded {
                cause: Bounded::Payload(cause),
                ..
            } => Cause::Payload(cause),
            Self::Bounded {
                cause: Bounded::Wire(cause),
                ..
            } => Cause::Wire(cause),
            Self::Values { cause, .. } => Cause::Values { cause },
        }
    }
}
