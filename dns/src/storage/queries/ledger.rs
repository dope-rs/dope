use std::{mem, time};

use o3::collections::batch::set;

use crate::{
    config, discovery,
    storage::{
        queries,
        state::{self, indices, streams},
    },
    wire::{self, response},
};

pub(super) struct Ledger<const LANES: usize, const ADDRESSES: usize> {
    pub(super) slots: Box<[state::State<ADDRESSES>]>,
    pub(super) free_lanes: set::Set<indices::LaneIndex>,
    pub(super) registrations: Registrations,
}

pub(super) struct Registrations {
    pub(super) pending: queries::PendingIndex,
    pub(super) stream_live: [usize; config::MAX_SERVERS],
    pub(super) transactions: queries::Transactions,
}

pub(super) struct Started {
    pub(super) key: state::Key,
    pub(super) family: state::Family,
    pub(super) transaction: wire::TransactionId,
    pub(super) server: indices::ServerIndex,
}

pub(super) enum Start {
    Started(Started),
    Processed,
}

#[must_use = "a located DNS request remains pending until acquired"]
pub(super) struct PendingStart<'inner, const ADDRESSES: usize> {
    claim: queries::Claim<'inner>,
    queue: queries::PendingQueue,
    slots: &'inner mut [state::State<ADDRESSES>],
    stream_live: &'inner mut [usize; config::MAX_SERVERS],
    transactions: &'inner mut queries::Transactions,
    now: time::Instant,
    timeout: time::Duration,
}

#[must_use = "a claimed waiting DNS request must be transitioned or rolled back"]
pub(super) struct WaitingClaim<'inner, const ADDRESSES: usize> {
    pub(super) slot: &'inner mut state::FamilyState<ADDRESSES>,
    pub(super) registrations: &'inner mut Registrations,
    pub(super) key: state::Key,
    pub(super) family_key: indices::FamilyKey,
    pub(super) request: state::Request,
}

impl Registrations {
    pub(super) fn index_new_pending<const N: usize>(
        &self,
        key: indices::FamilyKey,
        value: &state::FamilyState<N>,
    ) {
        if let state::FamilyState::Pending(request) = value {
            self.pending.register(key, request);
        }
    }

    fn index_transition<const N: usize>(
        &self,
        key: indices::FamilyKey,
        value: state::FamilyState<N>,
    ) -> state::FamilyState<N> {
        if let state::FamilyState::Pending(request) = &value {
            self.pending.register(key, request);
        }
        value
    }

    fn retire<const N: usize>(&mut self, key: indices::FamilyKey, value: state::FamilyState<N>) {
        match value {
            state::FamilyState::Pending(request) => {
                self.pending.unregister(key, &request);
                request.release(self);
            }
            state::FamilyState::Waiting {
                request,
                transaction,
                ..
            } => {
                request.release(self);
                self.transactions.release(transaction);
            }
            state::FamilyState::Claimed
            | state::FamilyState::Done { .. }
            | state::FamilyState::Empty
            | state::FamilyState::Failed(_) => {}
        }
    }
}

impl streams::Registry for Registrations {
    fn acquire(&mut self, server: indices::ServerIndex) -> streams::Lease {
        self.stream_live[server.get()] += 1;
        streams::Lease::acquired()
    }

    fn transfer(
        &mut self,
        lease: streams::Lease,
        from: indices::ServerIndex,
        to: indices::ServerIndex,
    ) -> streams::Lease {
        if from != to {
            self.stream_live[from.get()] -= 1;
            self.stream_live[to.get()] += 1;
        }
        lease
    }

    fn release(&mut self, lease: streams::Lease, server: indices::ServerIndex) {
        let _lease = lease;
        self.stream_live[server.get()] -= 1;
    }
}

fn restore_pending<const ADDRESSES: usize>(
    slot: &mut state::FamilyState<ADDRESSES>,
    registrations: &mut Registrations,
    family_key: indices::FamilyKey,
    request: state::Request,
) {
    registrations.pending.register(family_key, &request);
    *slot = state::FamilyState::Pending(request);
}

impl<const ADDRESSES: usize> WaitingClaim<'_, ADDRESSES> {
    pub(super) fn apply(self, outcome: wire::Outcome<ADDRESSES>, retry: state::RetryPolicy) {
        self.transition(|request, streams| {
            state::FamilyState::apply(outcome, request, retry, streams)
        });
    }

    pub(super) fn rollback(self) {
        let Self {
            slot,
            registrations,
            family_key,
            request,
            ..
        } = self;
        restore_pending(slot, registrations, family_key, request);
    }

    pub(super) fn timeout(self, retry: state::RetryPolicy) {
        self.transition(|request, streams| state::FamilyState::timeout(request, retry, streams));
    }

    pub(super) fn malformed(self, retry: state::RetryPolicy) {
        self.transition(|request, streams| state::FamilyState::malformed(request, retry, streams));
    }

    pub(super) fn reject(self, rejection: response::Rejection) {
        self.transition(|request, streams| state::FamilyState::reject(rejection, request, streams));
    }

    fn transition(
        self,
        transition: impl FnOnce(state::Request, &mut Registrations) -> state::FamilyState<ADDRESSES>,
    ) {
        let Self {
            slot,
            registrations,
            family_key,
            request,
            ..
        } = self;
        let next = transition(request, registrations);
        *slot = registrations.index_transition(family_key, next);
    }
}

impl<const ADDRESSES: usize> PendingStart<'_, ADDRESSES> {
    pub(super) fn acquire(self) -> Option<Start> {
        let Self {
            claim,
            queue,
            slots,
            stream_live,
            transactions,
            now,
            timeout,
        } = self;
        let family_key = claim.key();
        let family = family_key.family();
        let lane = family_key.lane();
        let Some(deadline) = now.checked_add(timeout) else {
            let state::State::Query(query) = slots.get_mut(lane.get())? else {
                return None;
            };
            let key = state::Key {
                lane,
                generation: query.generation,
            };
            let slot = query.family_mut(family);
            let current = mem::replace(slot, state::FamilyState::Claimed);
            let state::FamilyState::Pending(request) = current else {
                *slot = current;
                return None;
            };
            debug_assert!(queue.matches(&request));
            request.release(stream_live);
            *slot = state::FamilyState::Failed(discovery::ErrorKind::Deadline);
            complete_if_terminal(slots, key, now);
            claim.commit();
            return Some(Start::Processed);
        };

        let state::State::Query(query) = slots.get_mut(lane.get())? else {
            return None;
        };
        let key = state::Key {
            lane,
            generation: query.generation,
        };
        let slot = query.family_mut(family);
        let current = mem::replace(slot, state::FamilyState::Claimed);
        let state::FamilyState::Pending(request) = current else {
            *slot = current;
            return None;
        };
        let Some(transaction) = transactions.allocate(family_key) else {
            *slot = state::FamilyState::Pending(request);
            return None;
        };
        debug_assert!(queue.matches(&request));
        let server = request.server();
        let transaction_id = transaction.id();
        *slot = state::FamilyState::Waiting {
            request,
            transaction,
            deadline,
        };
        claim.commit();
        Some(Start::Started(Started {
            key,
            family,
            transaction: transaction_id,
            server,
        }))
    }
}

impl<const LANES: usize, const ADDRESSES: usize> Ledger<LANES, ADDRESSES> {
    pub(super) fn pending_start(
        &mut self,
        queue: queries::PendingQueue,
        now: time::Instant,
        timeout: time::Duration,
    ) -> Option<PendingStart<'_, ADDRESSES>> {
        let Self {
            slots,
            registrations,
            ..
        } = self;
        let Registrations {
            pending,
            stream_live,
            transactions,
        } = registrations;
        Some(PendingStart {
            claim: pending.claim(queue)?,
            queue,
            slots,
            stream_live,
            transactions,
            now,
            timeout,
        })
    }

    pub(super) fn claim_waiting(
        &mut self,
        key: state::Key,
        family: state::Family,
        accept: impl FnOnce(wire::TransactionId, time::Instant) -> bool,
    ) -> Option<WaitingClaim<'_, ADDRESSES>> {
        let family_key = indices::FamilyKey::new(key.lane, family);
        let Self {
            slots,
            registrations,
            ..
        } = self;
        let state::State::Query(query) = slots.get_mut(key.lane.get())? else {
            return None;
        };
        if query.generation != key.generation {
            return None;
        }
        let slot = query.family_mut(family);
        let current = mem::replace(slot, state::FamilyState::Claimed);
        let state::FamilyState::Waiting {
            request,
            transaction,
            deadline,
        } = current
        else {
            *slot = current;
            return None;
        };
        if !accept(transaction.id(), deadline) {
            *slot = state::FamilyState::Waiting {
                request,
                transaction,
                deadline,
            };
            return None;
        }
        registrations.transactions.release(transaction);
        Some(WaitingClaim {
            slot,
            registrations,
            key,
            family_key,
            request,
        })
    }

    pub(super) fn complete_if_terminal(&mut self, key: state::Key, now: time::Instant) {
        complete_if_terminal(&mut self.slots, key, now);
    }

    pub(super) fn retire_lane(&mut self, lane: indices::LaneIndex) {
        let Self {
            slots,
            registrations,
            ..
        } = self;
        let generation = slots[lane.get()].generation();
        let previous = mem::replace(&mut slots[lane.get()], state::State::Vacant { generation });
        if let state::State::Query(query) = previous {
            for (family, state) in query.into_families() {
                registrations.retire(indices::FamilyKey::new(lane, family), state);
            }
        }
    }
}

fn complete_if_terminal<const ADDRESSES: usize>(
    slots: &mut [state::State<ADDRESSES>],
    key: state::Key,
    now: time::Instant,
) {
    let result = {
        let Some(state::State::Query(query)) = slots.get(key.lane.get()) else {
            return;
        };
        if query.generation != key.generation || !query.terminal() {
            return;
        }
        query.complete(now)
    };
    slots[key.lane.get()] = state::State::Complete {
        generation: key.generation,
        result,
    };
}
