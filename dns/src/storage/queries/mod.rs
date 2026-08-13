use std::{cell, io, mem, net, process, time};

use dope::{core::driver::schedule, manifold::connector::app, net::link::egress::data};
use o3::collections;

use crate::{
    config, discovery,
    storage::{
        failure,
        state::{self, indices},
    },
    wire::{self, query, response},
};

mod ledger;
mod sealed;

pub(super) use sealed::{Claim, PendingIndex, PendingQueue, TransactionLease, Transactions};

pub(crate) struct Queries<const LANES: usize, const ADDRESSES: usize> {
    inner: cell::RefCell<ledger::Ledger<LANES, ADDRESSES>>,
    pub(super) config: config::Config,
}

pub(crate) struct Lane<'queries, const LANES: usize, const ADDRESSES: usize> {
    queries: &'queries Queries<LANES, ADDRESSES>,
    index: indices::LaneIndex,
}

impl<const LANES: usize, const ADDRESSES: usize> Drop for Lane<'_, LANES, ADDRESSES> {
    fn drop(&mut self) {
        let mut inner = self.queries.inner.borrow_mut();
        inner.retire_lane(self.index);
        if !inner.free_lanes.insert(self.index) {
            process::abort();
        }
    }
}

pub(crate) enum Start<'queries, const LANES: usize, const ADDRESSES: usize> {
    Prepared(Prepared<'queries, LANES, ADDRESSES>),
    Processed,
}

/// A transaction whose uncommitted drop restores its request and ID.
#[must_use = "a prepared DNS transaction must be committed after submission"]
pub(crate) struct Prepared<'queries, const LANES: usize, const ADDRESSES: usize> {
    queries: &'queries Queries<LANES, ADDRESSES>,
    key: state::Key,
    family: state::Family,
    transaction: wire::TransactionId,
    server: net::SocketAddr,
    armed: bool,
}

impl<const LANES: usize, const ADDRESSES: usize> Queries<LANES, ADDRESSES> {
    pub(super) fn try_new(
        config: config::Config,
        indices: indices::IndexLayout,
    ) -> Result<Self, collections::AllocationError> {
        let inner = ledger::Ledger {
            slots: collections::BoxSliceExt::try_box_with(LANES, |_| state::State::vacant())?,
            free_lanes: indices.lanes().try_filled_set()?,
            registrations: ledger::Registrations {
                pending: PendingIndex::try_new(indices.families())?,
                stream_live: [0; config::MAX_SERVERS],
                transactions: Transactions::try_new(indices.transactions())?,
            },
        };
        Ok(Self {
            inner: cell::RefCell::new(inner),
            config,
        })
    }

    pub(super) fn seed(&self, seed: u64) {
        self.inner
            .borrow_mut()
            .registrations
            .transactions
            .seed(seed);
    }

    fn retry_policy(&self) -> state::RetryPolicy {
        state::RetryPolicy::new(self.config.query.attempts, self.config.servers.count())
    }

    pub(super) fn claim(&self) -> io::Result<Lane<'_, LANES, ADDRESSES>> {
        let Some(index) = self.inner.borrow_mut().free_lanes.pop() else {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "DNS discovery lane capacity is exhausted",
            ));
        };
        Ok(Lane {
            queries: self,
            index,
        })
    }

    pub(crate) fn admit_datagram<'queries, T>(
        &'queries self,
        now: time::Instant,
        work: schedule::Application<'_, '_>,
        value: T,
    ) -> schedule::Admission<(T, Start<'queries, LANES, ADDRESSES>)> {
        let mut inner = self.inner.borrow_mut();
        let Some(front) =
            inner.pending_start(PendingQueue::Datagram, now, self.config.query.timeout)
        else {
            return schedule::Admission::Empty;
        };
        let admission = work.admit_with(|| {
            Some((
                value,
                match front.acquire()? {
                    ledger::Start::Started(started) => Start::Prepared(self.prepared(started)),
                    ledger::Start::Processed => Start::Processed,
                },
            ))
        });
        drop(inner);
        admission
    }

    pub(crate) fn admit_stream<'queries, 'permit, 'queue, 'd, B>(
        &'queries self,
        now: time::Instant,
        server: net::SocketAddr,
        drain: &'permit mut app::RequestDrain<'queue, 'd, B>,
    ) -> app::RequestAdmission<'permit, 'queue, 'd, B, Start<'queries, LANES, ADDRESSES>>
    where
        B: data::Payload<'d>,
    {
        let Some(server) = self.server_index(server) else {
            return app::RequestAdmission::Empty;
        };
        let queue = PendingQueue::Stream(server);
        let mut inner = self.inner.borrow_mut();
        let front = inner.pending_start(queue, now, self.config.query.timeout);
        let admission = drain.admit_with(front, |front| {
            Some(match front.acquire()? {
                ledger::Start::Started(started) => Start::Prepared(self.prepared(started)),
                ledger::Start::Processed => Start::Processed,
            })
        });
        drop(inner);
        admission
    }

    pub(crate) fn needs_datagram(&self) -> bool {
        self.inner.borrow().registrations.pending.needs_datagram()
    }

    pub(crate) fn needs_stream(&self) -> bool {
        self.inner.borrow().registrations.pending.needs_stream()
    }

    pub(crate) fn has_stream_work(&self) -> bool {
        self.inner
            .borrow()
            .registrations
            .stream_live
            .iter()
            .any(|live| *live != 0)
    }

    pub(crate) fn has_stream_work_for(&self, server: net::SocketAddr) -> bool {
        let Some(server) = self.server_index(server) else {
            return false;
        };
        self.inner.borrow().registrations.stream_live[server.get()] != 0
    }

    pub(crate) fn accept_datagram(
        &self,
        source: net::SocketAddr,
        packet: &[u8],
        now: time::Instant,
    ) {
        self.accept(Some(source), state::Transport::Datagram, packet, now);
    }

    pub(crate) fn accept_stream(&self, server: net::SocketAddr, packet: &[u8], now: time::Instant) {
        self.accept(Some(server), state::Transport::Stream, packet, now);
    }

    fn prepared(&self, started: ledger::Started) -> Prepared<'_, LANES, ADDRESSES> {
        let server = self.config.servers.as_slice()[started.server.get()];
        Prepared {
            queries: self,
            key: started.key,
            family: started.family,
            transaction: started.transaction,
            server,
            armed: true,
        }
    }

    fn accept(
        &self,
        source: Option<net::SocketAddr>,
        transport: state::Transport,
        packet: &[u8],
        now: time::Instant,
    ) {
        let Some(transaction) = response::Response::id(packet) else {
            return;
        };
        let incoming = (|| {
            let inner = self.inner.borrow();
            let family_key = inner.registrations.transactions.owner(transaction)?;
            let lane = family_key.lane();
            let family = family_key.family();
            let state::State::Query(query) = &inner.slots[lane.get()] else {
                return None;
            };
            let key = state::Key {
                lane,
                generation: query.generation,
            };
            let current = query.family(family);
            let request = current.waiting(transaction, transport)?;
            let expected_server = self.config.servers.as_slice()[request.server().get()];
            if source.is_some_and(|source| source != expected_server) {
                return None;
            }
            let decoded =
                response::Response::new(packet, transaction, &request.name, family.record_type())
                    .decode::<ADDRESSES>();
            if decoded.is_err() && transport == state::Transport::Datagram {
                return None;
            }
            Some((key, family, decoded))
        })();
        let Some((key, family, decoded)) = incoming else {
            return;
        };
        let retry = self.retry_policy();
        let mut inner = self.inner.borrow_mut();
        let Some(claim) = inner.claim_waiting(key, family, |active, _| active == transaction)
        else {
            return;
        };
        let key = claim.key;
        match decoded {
            Ok(response::Decoded::Outcome(outcome)) => claim.apply(outcome, retry),
            Ok(response::Decoded::Rejected(rejection)) => claim.reject(rejection),
            Err(_) => claim.malformed(retry),
        }
        inner.complete_if_terminal(key, now);
    }

    fn rollback(&self, key: state::Key, family: state::Family, transaction: wire::TransactionId) {
        let mut inner = self.inner.borrow_mut();
        if let Some(claim) = inner.claim_waiting(key, family, |active, _| active == transaction) {
            claim.rollback();
        }
    }

    fn server_index(&self, server: net::SocketAddr) -> Option<indices::ServerIndex> {
        self.config
            .servers
            .as_slice()
            .iter()
            .position(|candidate| *candidate == server)
            .and_then(indices::ServerIndex::new)
    }
}

impl<'queries, const LANES: usize, const ADDRESSES: usize> Prepared<'queries, LANES, ADDRESSES> {
    pub(crate) fn with_query<R>(
        &self,
        encode: impl for<'name> FnOnce(query::Query<'name>) -> R,
    ) -> Option<R> {
        let inner = self.queries.inner.borrow();
        let state::State::Query(state_query) = inner.slots.get(self.key.lane.get())? else {
            return None;
        };
        if state_query.generation != self.key.generation {
            return None;
        }
        let state::FamilyState::Waiting {
            request,
            transaction,
            ..
        } = state_query.family(self.family)
        else {
            return None;
        };
        if transaction.id() != self.transaction {
            return None;
        }
        Some(encode(query::Query::new(
            self.transaction,
            &request.name,
            self.family.record_type(),
        )))
    }

    pub(crate) const fn server(&self) -> net::SocketAddr {
        self.server
    }

    pub(crate) fn commit(mut self) {
        self.armed = false;
    }
}

impl<const LANES: usize, const ADDRESSES: usize> Drop for Prepared<'_, LANES, ADDRESSES> {
    fn drop(&mut self) {
        if self.armed {
            self.queries
                .rollback(self.key, self.family, self.transaction);
        }
    }
}

impl<'queries, const LANES: usize, const ADDRESSES: usize> Lane<'queries, LANES, ADDRESSES> {
    pub(crate) fn start(
        &self,
        name: &crate::Name,
        now: time::Instant,
        started_unix_ns: crate::Timestamp,
    ) -> io::Result<state::Key> {
        let mut inner = self.queries.inner.borrow_mut();
        let generation = inner.slots[self.index.get()].next_generation()?;
        inner.retire_lane(self.index);
        let query = state::Query::new(generation, now, started_unix_ns, name);
        let key = state::Key {
            lane: self.index,
            generation,
        };
        for family in state::Family::ALL {
            let family_key = indices::FamilyKey::new(self.index, family);
            inner
                .registrations
                .index_new_pending(family_key, query.family(family));
        }
        inner.slots[self.index.get()] = state::State::Query(query);
        Ok(key)
    }

    pub(crate) fn poll(&self, key: state::Key, now: time::Instant) -> state::Poll<ADDRESSES> {
        if key.lane != self.index {
            return state::Poll::Complete(Err(Self::malformed()));
        }
        let retry = self.queries.retry_policy();
        let mut inner = self.queries.inner.borrow_mut();
        if inner.slots[self.index.get()].generation() != key.generation {
            return state::Poll::Complete(Err(Self::malformed()));
        }
        for family in state::Family::ALL {
            if let Some(claim) = inner.claim_waiting(key, family, |_, deadline| deadline <= now) {
                claim.timeout(retry);
            }
        }
        inner.complete_if_terminal(key, now);
        let value = &mut inner.slots[self.index.get()];
        match value {
            state::State::Query(query) => state::Poll::Pending(query.deadline(now)),
            state::State::Complete { .. } => {
                let previous = mem::replace(
                    value,
                    state::State::Vacant {
                        generation: key.generation,
                    },
                );
                let state::State::Complete { result, .. } = previous else {
                    return state::Poll::Complete(Err(Self::malformed()));
                };
                state::Poll::Complete(result)
            }
            state::State::Vacant { .. } => state::Poll::Complete(Err(Self::malformed())),
        }
    }

    fn malformed() -> crate::Failure {
        let timestamp = crate::Timestamp::now();
        crate::Failure::new(
            discovery::ErrorKind::Malformed,
            failure::FailureTimes::at(timestamp),
        )
    }
}
