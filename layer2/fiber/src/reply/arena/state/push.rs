//! Resource-accounted reply insertion.

use o3::depot::{self, arena::slab, index::Initialized};
use state::order::{Front, Lease, Slot};

use crate::reply::{
    self,
    arena::state,
    layout, order, recv,
    retain::{grant::payload, pool},
    slot, usage,
};

enum Reject {
    Front(order::Front),
    Capacity(usage::Capacity),
    Backpressure(usage::Backpressure),
}

impl Reject {
    /// A collected reply cancels the wait installed by its synchronous denial.
    /// Queue links remain for the existing bounded cancellation drain.
    fn acquire(
        cause: pool::Acquire,
        accept: recv::Acceptance,
        lane: pool::Lane<'_, '_, '_>,
    ) -> Self {
        match cause {
            pool::Acquire::Backpressure(cause) => {
                if let recv::Acceptance::Collected = accept {
                    lane.cancel();
                    Self::Capacity(usage::Capacity::Unavailable(cause))
                } else {
                    Self::Backpressure(cause)
                }
            }
            pool::Acquire::Capacity(cause) => Self::Capacity(cause),
        }
    }

    fn into_error<T>(self, value: T) -> usage::InsertError<T> {
        match self {
            Self::Front(front) => usage::InsertError::Front { value, front },
            Self::Capacity(cause) => usage::InsertError::Capacity { value, cause },
            Self::Backpressure(cause) => usage::InsertError::Backpressure { value, cause },
        }
    }
}

/// One selected reply front retained through inspection and insertion.
pub struct Push<'a, 'arena, 'credits, 'region, D, E = D, K = ()> {
    front: Option<(
        Front<'a, 'credits, K>,
        state::EntryKey,
        &'a mut state::Entry<'arena, 'credits, 'region, D, E>,
    )>,
    limit: &'a layout::Limit,
    lane: pool::Lane<'arena, 'credits, 'region>,
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Push<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) fn new(
        state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        let front = match state.order.route(lane.index).front() {
            Some(mut front) => {
                let key = front.value().key;
                // SAFETY: Book inserts the entry before linking its order item;
                // this retained front prevents that entry from being reclaimed.
                let live = unsafe {
                    depot::raw::Proof::<_, Initialized<state::Ordered<'credits, K>>>::new((
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
            limit: &state.limit,
            lane: pool::Lane::new(lane),
        }
    }

    pub fn tag(&mut self) -> Option<&K> {
        Some(&self.front.as_mut()?.0.value().tag)
    }

    pub fn front(&self) -> order::Front {
        let Some((_, _, entry)) = &self.front else {
            return order::Front::Empty;
        };
        match entry.body {
            state::EntryBody::Attached(_) => match entry.acceptance {
                recv::Acceptance::Payload | recv::Acceptance::Collected => order::Front::Open,
                recv::Acceptance::Unit => order::Front::Denied,
            },
            state::EntryBody::Draining(_) | state::EntryBody::Drained => order::Front::Detached,
        }
    }

    pub(super) fn head(self) -> state::Head<'a, 'arena, 'credits, 'region, D, E, K> {
        let Some((front, key, entry)) = self.front else {
            return state::Head::Empty(order::Depth::ZERO);
        };
        let state::Entry {
            body,
            acceptance: accept,
            owners,
            signal,
        } = entry;
        let order = Slot::new(front, key, owners);
        match body {
            state::EntryBody::Attached(state) => {
                state::Head::Attached(reply::Open::new(order, *accept, state.state(), signal))
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
        usage: usage::Usage,
    ) -> Result<slot::Push<'a, 'arena, 'credits, 'region, D, K>, Reject> {
        let limit = *self.limit;
        let lane = self.lane;
        let open = self.attached().map_err(Reject::Front)?;
        let accept = open.acceptance();
        if let recv::Acceptance::Unit = accept {
            return Err(Reject::Front(order::Front::Denied));
        }
        match open.try_data(limit, usage) {
            Ok(held) => Ok(held),
            Err(cause) => Err(Reject::acquire(cause, accept, lane)),
        }
    }

    fn try_failure(
        self,
        usage: usage::Usage,
    ) -> Result<Failure<'a, 'arena, 'credits, 'region, E, K>, Reject> {
        let limit = *self.limit;
        let lane = self.lane;
        let open = self.attached().map_err(Reject::Front)?;
        let accept = open.acceptance();
        let (order, slot, signal) = open.try_failure(limit, usage).map_err(Reject::Capacity)?;
        let lease = match order.try_lease(payload::Failure::new(usage)) {
            Ok(lease) => lease,
            Err(cause) => {
                return Err(Reject::acquire(cause, accept, lane));
            }
        };
        Ok(Failure {
            slot,
            lease,
            signal,
        })
    }

    pub fn try_push(self, item: D) -> Result<(), usage::InsertError<D>>
    where
        D: usage::Footprint,
    {
        let usage = item.footprint();
        let held = match self.try_data(usage) {
            Ok(held) => held,
            Err(reject) => return Err(reject.into_error(item)),
        };
        held.commit(item);
        Ok(())
    }

    pub fn try_build<B>(self, builder: B) -> Result<(), usage::PushError<B, B::Error, D>>
    where
        D: usage::Footprint,
        B: usage::Build<D>,
    {
        let usage = builder.usage();
        let held = match self.try_data(usage) {
            Ok(held) => held,
            Err(Reject::Backpressure(cause)) => {
                return Err(usage::PushError::Backpressure { builder, cause });
            }
            Err(Reject::Capacity(cause)) => {
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
        let actual = item.footprint();
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
        E: usage::Footprint,
    {
        let usage = item.footprint();
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
