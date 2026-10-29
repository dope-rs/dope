use std::time;

use crate::{
    lookup::error,
    query::answer::addr,
    storage::queries::{state, tx},
};

pub(in crate::storage::queries) enum Branch<'s, const N: usize> {
    Pending(state::Attempt<'s>),
    Waiting {
        attempt: state::Attempt<'s>,
        transaction: tx::Lease<'s>,
    },
    Done {
        addresses: addr::Addresses<N>,
        valid_until: time::Instant,
    },
    Empty,
    Failed(error::Kind),
}

pub(in crate::storage::queries::state) enum Outcome<const N: usize> {
    Done {
        addresses: addr::Addresses<N>,
        valid_until: time::Instant,
    },
    Empty,
    Failed(error::Kind),
}

impl<'s, const N: usize> Branch<'s, N> {
    pub(in crate::storage::queries) fn is_stream(&self) -> bool {
        match self {
            Self::Pending(probe) | Self::Waiting { attempt: probe, .. } => {
                probe.request().stream().is_some()
            }
            Self::Done { .. } | Self::Empty | Self::Failed(_) => false,
        }
    }

    pub(in crate::storage::queries) fn deadline(&self) -> Option<time::Instant> {
        match self {
            Self::Pending(probe) | Self::Waiting { attempt: probe, .. } => Some(probe.deadline()),
            Self::Done { .. } | Self::Empty | Self::Failed(_) => None,
        }
    }
}
