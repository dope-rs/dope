//! Resource-accounted reply insertion.

use o3::{
    base::cell::region,
    depot::{self, arena::slab, index::Initialized},
};
use state::order::{Front, Slot};

use crate::reply::{
    self,
    arena::state,
    order, recv,
    retain::pool,
    slot::{self, End},
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
            Some(front) => {
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
                Some((front, entry.into_mut()))
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
            .is_some_and(|(front, _)| Some(front.value().key()) == cutoff.key())
    }

    pub fn front(&self) -> order::Front {
        let Some((_, entry)) = &self.front else {
            return order::Front::Empty;
        };
        match entry.state.end {
            End::Open => match entry.owners.acceptance {
                Some(recv::Acceptance::Unit) => order::Front::Denied,
                _ => order::Front::Open,
            },
            _ => order::Front::Detached,
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
        let Some((front, entry)) = self.front else {
            return state::Head::Empty(order::Depth::ZERO);
        };
        let state::Entry {
            state,
            owners,
            signal,
        } = entry;
        let order = Slot::new(front, owners);
        match state.end {
            End::Open => state::Head::Attached(reply::Open::new(order, state, signal)),
            End::Drained => state::Head::Drained(order),
            _ => state::Head::Draining(order),
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
    ) -> Result<Option<slot::Push<'a, 'arena, 'credits, 'region, D>>, Reject> {
        let open = self.attached().map_err(Reject::Front)?;
        let Some(accept) = open.acceptance() else {
            return Ok(None);
        };
        if let recv::Acceptance::Unit = accept {
            return Err(Reject::Front(order::Front::Denied));
        }
        open.try_data(usage).map(Some).map_err(Reject::Acquire)
    }

    /// Retains the attached front before constructing its terminal error.
    pub fn try_failure(
        self,
    ) -> Result<reply::Failure<'a, 'arena, 'credits, 'region, E, K>, order::Front> {
        Ok(self.attached()?.failure())
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
        let failure = match self.try_failure() {
            Ok(failure) => failure,
            Err(front) => return Err(Reject::Front(front).into_error(item)),
        };
        failure
            .try_insert(item, usage)
            .map_err(|(value, cause)| Reject::Acquire(cause).into_error(value))
    }
}
