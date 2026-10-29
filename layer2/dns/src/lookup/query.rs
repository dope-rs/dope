use dope::core::driver::schedule::clock;
use o3::base::{
    cell::region,
    time::{Nanos, Unix},
};

use crate::{
    lookup::{self, error},
    storage::queries::{
        self,
        state::{self, poll},
    },
};

pub(super) struct Query<'storage, 's, 'd, const LANES: usize, const ADDRESSES: usize> {
    name: Box<crate::host::Name>,
    lane: queries::Lane<'storage, 's, 'd, LANES, ADDRESSES>,
}

pub(super) enum QueryPoll<'d, const ADDRESSES: usize> {
    Idle,
    Pending(Option<clock::Deadline<'d>>),
    Complete {
        reason: lookup::Reason,
        completed_at: clock::Moment<'d>,
        started_unix_ns: Unix<Nanos>,
        result: Result<state::Answer<ADDRESSES>, error::Kind>,
    },
}

impl<'storage, 's, 'd, const LANES: usize, const ADDRESSES: usize>
    Query<'storage, 's, 'd, LANES, ADDRESSES>
{
    pub(super) fn new(
        name: Box<crate::host::Name>,
        lane: queries::Lane<'storage, 's, 'd, LANES, ADDRESSES>,
    ) -> Self {
        Self { name, lane }
    }

    pub(super) fn name(&self) -> &crate::host::Name {
        &self.name
    }

    pub(super) fn is_ready(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        self.lane.is_ready(token)
    }

    pub(super) fn poll(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: clock::Moment<'d>,
    ) -> QueryPoll<'d, ADDRESSES> {
        use poll::State;

        let polled = now.at(&self.lane).apply(|lane, raw| lane.poll(token, raw));
        match polled {
            State::Idle => QueryPoll::Idle,
            State::Pending(due) => QueryPoll::Pending(due.map(|at| now.deadline_at(at))),
            State::Complete(poll::Ready {
                reason,
                started_unix_ns: unix,
                completed_at: done,
                result,
            }) => QueryPoll::Complete {
                reason,
                completed_at: now.moment_at(done),
                started_unix_ns: unix,
                result,
            },
        }
    }

    pub(super) fn start(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        reason: lookup::Reason,
        now: clock::Moment<'d>,
        stamp: Unix<Nanos>,
    ) -> Result<(), error::Kind> {
        self.lane
            .start(token, reason, stamp, &self.name, now)
            .map_err(error::Kind::Deadline)
    }
}
