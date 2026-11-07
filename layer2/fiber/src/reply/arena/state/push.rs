//! Resource-accounted reply insertion.

use o3::{
    base::cell::region,
    depot::{self, arena::slab, index::Initialized},
};
use state::order::{Front, Lease, Slot};

use crate::reply::{
    self,
    arena::state,
    order, recv,
    retain::{grant::payload, pool},
    slot,
    usage::{self, Footprint, amount::Amount as _},
};

enum Reject {
    Front(order::Front),
    Acquire(pool::Acquire),
}

impl Reject {
    fn into_error<T>(self, value: T) -> usage::InsertError<T> {
        match self {
            Self::Front(front) => usage::InsertError::Front { value, front },
            Self::Acquire(pool::Acquire::Capacity(cause)) => {
                usage::InsertError::Capacity { value, cause }
            }
            Self::Acquire(pool::Acquire::Backpressure(cause)) => {
                usage::InsertError::Backpressure { value, cause }
            }
        }
    }
}

/// One selected reply front retained through inspection and insertion.
pub struct Push<'a, 'arena, 'credits, 'region, D: Footprint, E = D, K = ()> {
    front: Option<(
        Front<'a, K>,
        state::EntryKey,
        &'a mut state::Entry<'arena, 'credits, 'region, D, E>,
    )>,
    arena: &'a reply::Arena<'arena, 'credits, 'region, D, E, K>,
}

impl<'a, 'arena, 'credits, 'region, D: Footprint, E, K>
    Push<'a, 'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply::arena) fn new(
        lane: reply::Lane<'a, 'arena, 'credits, 'region, D, E, K>,
        token: &'a mut region::Token<region::Identity<'region>>,
    ) -> Self {
        let state = token.borrow_mut(&lane.arena.state);
        let front = match state.order.route(lane.index).front() {
            Some(mut front) => {
                let key = front.value().key();
                // SAFETY: Book inserts the entry before linking its order item;
                // this retained front prevents that entry from being reclaimed.
                let live = unsafe {
                    depot::raw::Proof::<_, Initialized<state::Ordered<K>>>::new((
                        (&mut state.slots.0, front),
                        key.index().into(),
                    ))
                };
                let (entry, front) = slab::Occupied::from_live(live);
                Some((front, key, entry.into_mut()))
            }
            None => None,
        };
        Self {
            front,
            arena: lane.arena,
        }
    }

    pub fn tag(&mut self) -> Option<&K> {
        Some(self.front.as_mut()?.0.value().tag())
    }

    pub fn is_cutoff(&self, cutoff: order::Cutoff<'arena>) -> bool {
        self.front
            .as_ref()
            .is_some_and(|(_, key, _)| Some(*key) == cutoff.key())
    }

    pub fn front(&self) -> order::Front {
        let Some((_, _, entry)) = &self.front else {
            return order::Front::Empty;
        };
        match entry.body {
            state::EntryBody::Attached(_) => match entry.owners.acceptance {
                Some(recv::Acceptance::Unit) => order::Front::Denied,
                _ => order::Front::Open,
            },
            state::EntryBody::Draining(_) | state::EntryBody::Drained => order::Front::Detached,
        }
    }

    pub fn complete(self) -> order::Depth {
        let arena = self.arena;
        match self.head() {
            state::Head::Attached(open) => open.complete(),
            state::Head::Draining(order) => order.reclaim(),
            state::Head::Drained(order) => {
                let lane = reply::Lane {
                    arena,
                    index: order.owner.lane(),
                };
                let (depth, retired) = order.retire();
                if let Some(retired) = retired {
                    lane.defer_retire(retired);
                }
                depth
            }
            state::Head::Empty(depth) => depth,
        }
    }

    pub(super) fn head(self) -> state::Head<'a, 'arena, 'credits, 'region, D, E, K> {
        let Some((front, key, entry)) = self.front else {
            return state::Head::Empty(order::Depth::ZERO);
        };
        let state::Entry {
            body,
            owners,
            signal,
        } = entry;
        let order = Slot::new(front, key, owners);
        match body {
            state::EntryBody::Attached(state) => {
                state::Head::Attached(reply::Open::new(order, state.state(), signal))
            }
            state::EntryBody::Draining(_) => state::Head::Draining(order),
            state::EntryBody::Drained => state::Head::Drained(order),
        }
    }

    fn attached(self) -> Result<reply::Open<'a, 'arena, 'credits, 'region, D, E, K>, order::Front> {
        match self.head() {
            state::Head::Empty(_) => Err(order::Front::Empty),
            state::Head::Draining(_) | state::Head::Drained(_) => Err(order::Front::Detached),
            state::Head::Attached(open) => Ok(open),
        }
    }

    fn try_data(
        self,
        usage: D::Amount,
    ) -> Result<Option<slot::Push<'a, 'arena, 'credits, 'region, D, K>>, Reject> {
        let limit = self.arena.limit;
        let open = self.attached().map_err(Reject::Front)?;
        let Some(accept) = open.acceptance() else {
            return Ok(None);
        };
        if let recv::Acceptance::Unit = accept {
            return Err(Reject::Front(order::Front::Denied));
        }
        open.try_data(limit, usage)
            .map(Some)
            .map_err(Reject::Acquire)
    }

    fn try_failure(
        self,
        usage: usage::Usage,
    ) -> Result<Failure<'a, 'arena, 'credits, 'region, E, K>, Reject> {
        let limit = self.arena.limit;
        let open = self.attached().map_err(Reject::Front)?;
        let (order, slot, signal) = open.failure();
        let lease = order
            .try_lease(payload::Failure::new(usage), limit)
            .map_err(Reject::Acquire)?;
        Ok(Failure {
            slot,
            lease,
            signal,
        })
    }

    pub fn try_push(self, item: D) -> Result<(), usage::InsertError<D>> {
        let usage = item.footprint();
        let held = match self.try_data(usage) {
            Ok(Some(held)) => held,
            Ok(None) => return Ok(()),
            Err(reject) => return Err(reject.into_error(item)),
        };
        held.commit(item);
        Ok(())
    }

    pub fn try_build<B>(self, builder: B) -> Result<(), usage::PushError<B, B::Error, D>>
    where
        B: usage::Build<D>,
    {
        let usage = builder.usage();
        let held = match self.try_data(usage) {
            Ok(Some(held)) => held,
            Ok(None) => return Ok(()),
            Err(Reject::Acquire(pool::Acquire::Backpressure(cause))) => {
                return Err(usage::PushError::Backpressure { builder, cause });
            }
            Err(Reject::Acquire(pool::Acquire::Capacity(cause))) => {
                return Err(usage::PushError::Capacity { builder, cause });
            }
            Err(Reject::Front(front)) => {
                return Err(usage::PushError::Front { builder, front });
            }
        };
        let item = match builder.try_build() {
            Ok(item) => item,
            Err((builder, error)) => {
                return Err(usage::PushError::Build { builder, error });
            }
        };
        let declared = held.usage();
        let actual = item.footprint().usage();
        if actual != declared {
            return Err(usage::PushError::Footprint {
                value: item,
                declared,
                actual,
            });
        }
        held.commit(item);
        Ok(())
    }

    pub fn try_fail(self, item: E) -> Result<order::Depth, usage::InsertError<E>>
    where
        E: Footprint,
    {
        let usage = item.footprint().usage();
        let held = match self.try_failure(usage) {
            Ok(held) => held,
            Err(reject) => return Err(reject.into_error(item)),
        };
        Ok(held.commit(item))
    }
}

struct Failure<'a, 'arena, 'credits, 'region, E, K> {
    slot: reply::Failure<'a, E>,
    lease: Lease<'a, 'arena, 'credits, 'region, payload::Failure, K>,
    signal: &'a mut state::Signal<'region>,
}

impl<E, K> Failure<'_, '_, '_, '_, E, K> {
    fn commit(self, value: E) -> order::Depth {
        let Self {
            slot,
            lease,
            signal,
        } = self;
        let amount = lease.amount();
        slot.commit(reply::Failed { value, amount });
        let depth = lease.discard();
        signal.notify();
        depth
    }
}
