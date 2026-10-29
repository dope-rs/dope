use dope_core::io::transfer;
use send::reclaim;

use crate::{
    link::{
        duplex::{self, sending},
        egress, event,
    },
    wire::send,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalid {
    Stale {
        actual: transfer::Len,
    },
    Excess {
        actual: transfer::Len,
        maximum: transfer::Len,
        source: event::SendSource,
    },
    Origin {
        actual: Option<event::SendSource>,
        expected: Option<event::SendSource>,
    },
    Overflow(reclaim::Overflow),
    Egress(egress::Invalid),
    SettleActive,
    SettleStale,
}

impl From<sending::Invalid> for Invalid {
    fn from(error: sending::Invalid) -> Self {
        match error {
            sending::Invalid::Stale { sent } => Self::Stale { actual: sent },
            sending::Invalid::Excess {
                sent,
                limit,
                origin,
            } => Self::Excess {
                actual: sent,
                maximum: limit,
                source: origin,
            },
        }
    }
}

impl From<duplex::QueueError> for Invalid {
    fn from(error: duplex::QueueError) -> Self {
        match error {
            duplex::QueueError::Origin { actual, expected } => Self::Origin { actual, expected },
            duplex::QueueError::Overflow(error) => Self::Overflow(error),
        }
    }
}
