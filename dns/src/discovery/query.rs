use std::{mem, time};

use storage::{queries, state};

use crate::{
    discovery,
    storage::{self, failure},
};

pub(super) struct InFlight {
    key: state::Key,
    reason: discovery::Reason,
}

enum Phase {
    Idle,
    InFlight(InFlight),
}

pub(super) struct Query<'d, const LANES: usize, const ADDRESSES: usize> {
    name: crate::Name,
    lane: queries::Lane<'d, LANES, ADDRESSES>,
    phase: Phase,
}

pub(super) enum QueryPoll<const ADDRESSES: usize> {
    Idle,
    Pending(Option<time::Instant>),
    Complete {
        reason: discovery::Reason,
        result: Result<state::Answer<ADDRESSES>, crate::Failure>,
    },
}

impl<'d, const LANES: usize, const ADDRESSES: usize> Query<'d, LANES, ADDRESSES> {
    pub(super) fn new(name: crate::Name, lane: queries::Lane<'d, LANES, ADDRESSES>) -> Self {
        Self {
            name,
            lane,
            phase: Phase::Idle,
        }
    }

    pub(super) fn name(&self) -> &crate::Name {
        &self.name
    }

    pub(super) fn poll(&mut self, now: time::Instant) -> QueryPoll<ADDRESSES> {
        let Phase::InFlight(in_flight) = mem::replace(&mut self.phase, Phase::Idle) else {
            return QueryPoll::Idle;
        };
        match self.lane.poll(in_flight.key, now) {
            state::Poll::Pending(poll_at) => {
                self.phase = Phase::InFlight(in_flight);
                QueryPoll::Pending(poll_at)
            }
            state::Poll::Complete(result) => QueryPoll::Complete {
                reason: in_flight.reason,
                result,
            },
        }
    }

    pub(super) fn start(
        &mut self,
        reason: discovery::Reason,
        now: time::Instant,
        started_unix_ns: crate::Timestamp,
    ) -> Result<(), crate::Failure> {
        let key = self
            .lane
            .start(&self.name, now, started_unix_ns)
            .map_err(|_| {
                crate::Failure::new(
                    discovery::ErrorKind::Malformed,
                    failure::FailureTimes::new(started_unix_ns, crate::Timestamp::now()),
                )
            })?;
        self.phase = Phase::InFlight(InFlight { key, reason });
        Ok(())
    }
}
