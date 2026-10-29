use std::{mem, time};

use dope::manifold::client::close;
use o3::buffer::pool;

use crate::{
    lookup::error,
    net::entropy,
    query::family,
    storage::queries::{Finish, Submission, index, ledger, state, stream, tx},
    wire::{frames, tx::Id},
};

#[must_use = "a located DNS request remains pending until acquired"]
pub(in crate::storage::queries) struct Udp<'ledger, 's, const N: usize> {
    inner: Candidate<'ledger, 's, N>,
}

#[must_use = "a located DNS request remains pending until acquired"]
pub(in crate::storage::queries) struct Tcp<'ledger, 's, const N: usize> {
    inner: Candidate<'ledger, 's, N>,
    id: Id,
}

struct Candidate<'ledger, 's, const N: usize> {
    claim: ledger::Claim<'ledger, 's>,
    slots: &'ledger mut [ledger::Slot<'s, N>],
    live: &'ledger mut stream::Live,
    ids: &'ledger mut tx::Table<'s>,
    now: time::Instant,
}

struct Ready<'ledger, 's, const N: usize> {
    claim: ledger::Claim<'ledger, 's>,
    slot: &'ledger mut state::Row<'s, N>,
    live: &'ledger mut stream::Live,
    ids: &'ledger mut tx::Table<'s>,
    lane: index::Lane,
    family: family::Version,
    probe: state::Attempt<'s>,
}

impl<'ledger, 's, const N: usize> Udp<'ledger, 's, N> {
    pub(super) fn new(
        claim: ledger::Claim<'ledger, 's>,
        slots: &'ledger mut [ledger::Slot<'s, N>],
        live: &'ledger mut stream::Live,
        ids: &'ledger mut tx::Table<'s>,
        now: time::Instant,
    ) -> Self {
        Self {
            inner: Candidate::new(claim, slots, live, ids, now),
        }
    }

    pub(in crate::storage::queries) fn start(
        self,
        random: &mut entropy::Datagram<'_>,
        buffer: pool::Cursor<pool::Width<{ frames::MAX_QUERY_BYTES }>>,
        submit: impl FnOnce(frames::Datagram, index::Server) -> Submission,
    ) -> Finish<Submission> {
        let Some(ready) = self.inner.take() else {
            return Finish::Done(Submission::Deferred);
        };
        let branch = index::Branch::new(ready.lane, ready.family);
        let req = ready.probe.request();
        let server = req.server();
        let name = req.name();
        let lease = match ready.ids.try_udp(branch, server, random) {
            Ok(lease) => lease,
            Err(cause) => {
                let lane = ready.fail(error::Kind::Identity(cause));
                return Finish::Complete {
                    value: Submission::Deferred,
                    lane,
                };
            }
        };
        let client = lease.client();
        let cookie = ready.ids.cookies(server);
        let id = Id::from_ordinal(lease.removed.index());
        let datagram =
            frames::Builder::new(id, name, ready.family).udp(buffer, lease.epoch(), client, cookie);
        let status = submit(datagram, server);
        match status {
            Submission::Committed => {
                ready.commit(lease.into());
                Finish::Done(status)
            }
            Submission::Deferred => {
                ready.rollback(lease);
                Finish::Done(status)
            }
            Submission::Failed(cause) => {
                ready.ids.release(lease.into());
                Finish::Complete {
                    value: status,
                    lane: ready.fail(cause),
                }
            }
        }
    }
}

impl<'ledger, 's, const N: usize> Tcp<'ledger, 's, N> {
    pub(super) fn new(
        claim: ledger::Claim<'ledger, 's>,
        slots: &'ledger mut [ledger::Slot<'s, N>],
        live: &'ledger mut stream::Live,
        ids: &'ledger mut tx::Table<'s>,
        now: time::Instant,
        id: Id,
    ) -> Self {
        Self {
            inner: Candidate::new(claim, slots, live, ids, now),
            id,
        }
    }

    pub(in crate::storage::queries) fn submit(
        self,
        submit: impl FnOnce(stream::Frame) -> stream::Admission,
    ) -> Finish<stream::Submission> {
        let Some(ready) = self.inner.take() else {
            return Finish::Done(stream::Submission::Deferred);
        };
        let branch = index::Branch::new(ready.lane, ready.family);
        let lease = match ready.ids.try_tcp(branch, self.id) {
            Ok(lease) => lease,
            Err(cause) => {
                return Finish::Complete {
                    value: stream::Submission::Deferred,
                    lane: ready.fail(error::Kind::Identity(cause)),
                };
            }
        };
        let req = ready.probe.request();
        let value = frames::Builder::new(self.id, req.name(), ready.family).tcp();
        let frame = stream::Frame::new(value, self.id);
        match submit(frame) {
            stream::Admission::Committed(commit) => {
                ready.commit(lease.commit(commit));
                Finish::Done(stream::Submission::Committed)
            }
            stream::Admission::Exhausted(frame) | stream::Admission::Saturated(frame) => {
                ready.defer(lease.cancel(frame.into_inner()));
                Finish::Done(stream::Submission::Deferred)
            }
            stream::Admission::Rejected(rejected) => {
                let (load, cause) = rejected.into_parts();
                Finish::Complete {
                    value: stream::Submission::Rejected,
                    lane: ready.reject(lease.cancel(load.into_inner()), error::Kind::Egress(cause)),
                }
            }
            stream::Admission::Unavailable(rejected) => {
                let (load, cause) = rejected.into_parts();
                Finish::Complete {
                    value: stream::Submission::Rejected,
                    lane: ready.reject(
                        lease.cancel(load.into_inner()),
                        error::Kind::Close {
                            reason: close::Reason::Egress(cause),
                        },
                    ),
                }
            }
        }
    }
}

impl<'ledger, 's, const N: usize> Candidate<'ledger, 's, N> {
    fn new(
        claim: ledger::Claim<'ledger, 's>,
        slots: &'ledger mut [ledger::Slot<'s, N>],
        live: &'ledger mut stream::Live,
        ids: &'ledger mut tx::Table<'s>,
        now: time::Instant,
    ) -> Self {
        Self {
            claim,
            slots,
            live,
            ids,
            now,
        }
    }

    fn take(self) -> Option<Ready<'ledger, 's, N>> {
        let Self {
            claim,
            slots,
            live,
            ids,
            now,
        } = self;
        let branch = index::Branch::from_ordinal(claim.index());
        let family = branch.family();
        let lane = branch.lane();
        let Some(ledger::Slot::Query(query)) = slots.get_mut(usize::from(lane)) else {
            claim.commit();
            return None;
        };
        let slot = query.branch_mut(family);
        let state::Branch::Pending(probe) = &slot.value else {
            claim.commit();
            return None;
        };
        if probe.deadline() <= now {
            return None;
        }
        let prev = mem::replace(&mut slot.value, state::Branch::Empty);
        let state::Branch::Pending(probe) = prev else {
            slot.value = prev;
            claim.commit();
            return None;
        };
        Some(Ready {
            claim,
            slot,
            live,
            ids,
            lane,
            family,
            probe,
        })
    }
}

impl<'ledger, 's, const N: usize> Ready<'ledger, 's, N> {
    fn commit(self, lease: tx::Lease<'s>) {
        let Self {
            claim, slot, probe, ..
        } = self;
        slot.value = state::Branch::Waiting {
            attempt: probe,
            transaction: lease,
        };
        claim.commit();
    }

    fn fail(self, cause: error::Kind) -> index::Lane {
        let Self {
            claim,
            slot,
            live,
            lane,
            probe,
            ..
        } = self;
        probe.release(live);
        slot.value = state::Branch::Failed(cause);
        claim.commit();
        lane
    }

    fn reject(self, cancel: tx::Cancel, cause: error::Kind) -> index::Lane {
        self.ids.cancel(cancel);
        self.fail(cause)
    }

    fn defer(self, cancel: tx::Cancel) {
        let Self {
            slot, ids, probe, ..
        } = self;
        ids.cancel(cancel);
        slot.value = state::Branch::Pending(probe);
    }

    fn rollback(self, lease: tx::Udp<'s>) {
        let Self {
            slot, ids, probe, ..
        } = self;
        ids.release(lease.into());
        slot.value = state::Branch::Pending(probe);
    }
}
