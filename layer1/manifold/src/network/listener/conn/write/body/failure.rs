use dope_net::link::egress::{self, load, reserve::exact};

use crate::network::listener::conn::write::{self, error};

/// Exact final-link failure retained until the connection's owner retires it.
pub enum Failure {
    Payload(load::Error),
    Capacity(write::Reject),
    Mismatch(error::Cause),
    Reserve(exact::Failure),
    Bound {
        charge: egress::Charge,
        maximum: usize,
    },
}

impl<B> From<egress::Excess<B>> for Failure {
    fn from(cause: egress::Excess<B>) -> Self {
        Self::Bound {
            charge: cause.load.charge(),
            maximum: cause.maximum,
        }
    }
}
