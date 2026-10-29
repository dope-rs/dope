use std::{mem, time};

use o3::depot::index::bitmap;

use crate::{
    lookup::error,
    query::family,
    storage::{
        self,
        queries::{
            Entry, index,
            ledger::{self, waiting},
            state::{self, poll},
        },
    },
};

pub(in crate::storage::queries) enum Slot<'s, const N: usize> {
    Idle {
        entries: [Entry<'s>; 2],
        completion: Option<poll::Ready<N>>,
    },
    Query(state::Query<'s, N>),
}

impl<'s, const N: usize> Slot<'s, N> {
    pub(in crate::storage::queries::ledger) fn try_new(
        store: &'s storage::Store,
        lane: index::Lane,
    ) -> Result<Self, bitmap::Error> {
        let four = store
            .pending
            .try_entry(index::Branch::new(lane, family::Version::Four).ordinal())?;
        let six = store
            .pending
            .try_entry(index::Branch::new(lane, family::Version::Six).ordinal())?;
        Ok(Self::Idle {
            entries: [four, six],
            completion: None,
        })
    }

    fn entries(&self) -> [Entry<'s>; 2] {
        match self {
            Self::Idle { entries, .. } => *entries,
            Self::Query(query) => query.entries(),
        }
    }

    pub(in crate::storage::queries) fn complete(&mut self, now: time::Instant) {
        let Self::Query(query) = self else {
            return;
        };
        let Some(result) = query.complete(now) else {
            return;
        };
        *self = Self::Idle {
            entries: query.entries(),
            completion: Some(poll::Ready {
                reason: query.reason,
                started_unix_ns: query.started_unix_ns,
                completed_at: now,
                result,
            }),
        };
    }

    /// Prioritizes stream failure over a timeout of the same attempt.
    pub(in crate::storage::queries) fn poll(
        &mut self,
        now: time::Instant,
        retry: state::Retry,
        reg: &mut ledger::Registry<'s>,
    ) -> poll::State<N> {
        use poll::State;

        if let Self::Query(query) = self {
            if let Some(cause) = reg.stream_failure {
                for family in family::Version::ALL {
                    let row = query.branch_mut(family);
                    if row.value.is_stream() {
                        reg.retire(row);
                        row.value = state::Branch::Failed(cause);
                    }
                }
            }
            for family in family::Version::ALL {
                let hard = query.hard_deadline;
                let slot = query.branch_mut(family);
                if !slot
                    .value
                    .deadline()
                    .is_some_and(|deadline| deadline <= now)
                {
                    continue;
                }
                match mem::replace(&mut slot.value, state::Branch::Empty) {
                    state::Branch::Waiting {
                        attempt: probe,
                        transaction: lease,
                    } => {
                        reg.ids.release(lease);
                        waiting::State::new(slot, reg, probe, hard).failed(
                            retry,
                            now,
                            error::Kind::Timeout,
                        );
                    }
                    state::Branch::Pending(probe) => {
                        slot.unregister(probe.request());
                        probe.retry(retry, hard, now, &mut reg.live, error::Kind::Timeout, slot);
                    }
                    previous => slot.value = previous,
                }
            }
            self.complete(now);
        }
        match self {
            Self::Query(query) => State::Pending(query.deadline()),
            Self::Idle {
                completion: done, ..
            } => match done.take() {
                Some(done) => State::Complete(done),
                None => State::Idle,
            },
        }
    }

    pub(in crate::storage::queries) fn is_ready(&self, stopped: bool) -> bool {
        match self {
            Self::Idle {
                completion: done, ..
            } => done.is_some(),
            Self::Query(query) => stopped && query.has_stream(),
        }
    }

    pub(in crate::storage::queries::ledger) fn start(
        &mut self,
        seed: state::Seed<'_, 's>,
        reg: &mut ledger::Registry<'s>,
    ) {
        self.retire(reg);
        *self = Self::Query(seed.into_query(self.entries()));
    }

    pub(in crate::storage::queries::ledger) fn retire(&mut self, reg: &mut ledger::Registry<'s>) {
        let idle = Self::Idle {
            entries: self.entries(),
            completion: None,
        };
        if let Self::Query(query) = mem::replace(self, idle) {
            for mut branch in query.into_iter() {
                reg.retire(&mut branch);
            }
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Slot<'static, 1>>() <= 784);
    assert!(mem::align_of::<Slot<'static, 1>>() == mem::align_of::<usize>());
};
