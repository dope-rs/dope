//! Resource-accounted reply insertion.

use o3::{
    base::cell::region,
    depot::{self, arena::slab, index::Initialized},
};
use state::order::{Front, Slot};

use crate::reply::{
    self,
    arena::state,
    input::{self, Reject},
    order, recv,
    retain::pool,
    slot::{self, End},
    usage::{self, Footprint, amount::Amount as _},
};

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

/// The ordered front and its original entry store.
pub struct Selection<'a, 'arena, 'credits, 'region, D: Footprint, E = D, K = ()> {
    front: Option<Front<'a, K>>,
    slots: &'a mut state::Entries<'arena, 'credits, 'region, D, E>,
    arena: &'a reply::Arena<'arena, 'credits, 'region, D, E, K>,
}

pub struct Push<'a, 'arena, 'credits, 'region, D: Footprint, E = D, K = ()> {
    front: Option<(
        Front<'a, K>,
        &'a mut state::Entry<'arena, 'credits, 'region, D, E>,
    )>,
    arena: &'a reply::Arena<'arena, 'credits, 'region, D, E, K>,
}

impl<'a, 'arena, 'credits, 'region, D: Footprint, E, K>
    Selection<'a, 'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply::arena) fn new(
        lane: reply::Lane<'a, 'arena, 'credits, 'region, D, E, K>,
        token: &'a mut region::Token<region::Identity<'region>>,
    ) -> Self {
        let state = token.borrow_mut(&lane.arena.state);
        Self {
            front: state.order.route(lane.index).front(),
            slots: &mut state.slots,
            arena: lane.arena,
        }
    }

    pub fn tag(&self) -> Option<&K> {
        Some(self.front.as_ref()?.value().tag())
    }

    pub fn is_cutoff(&self, cutoff: order::Cutoff<'arena>) -> bool {
        self.front
            .as_ref()
            .is_some_and(|front| Some(front.value().key()) == cutoff.key())
    }

    pub fn into_push(self) -> Push<'a, 'arena, 'credits, 'region, D, E, K> {
        let front = match self.front {
            Some(front) => {
                let index = front.value().key().index().into();
                // SAFETY: insertion precedes linking. The original front and disjoint
                // store loan exclude retirement or replacement of this entry.
                let live = unsafe {
                    depot::raw::Proof::<_, Initialized<state::Ordered<K>>>::new((
                        (&mut self.slots.0, front),
                        index,
                    ))
                };
                let (entry, front) = slab::Occupied::from_live(live);
                Some((front, entry.into_mut()))
            }
            None => None,
        };
        Push {
            front,
            arena: self.arena,
        }
    }
}

impl<'a, 'arena, 'credits, 'region, D: Footprint, E, K>
    Push<'a, 'arena, 'credits, 'region, D, E, K>
{
    pub fn front(&self) -> order::Front {
        let Some((_, entry)) = &self.front else {
            return order::Front::Empty;
        };
        match entry.state.end {
            End::Open => match entry.owners.acceptance {
                Some(recv::Accepted::Unit) => order::Front::Denied,
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

    pub fn input(self) -> Result<input::Target<'a, 'arena, 'credits, 'region, D>, order::Front> {
        let (_, entry) = self.front.ok_or(order::Front::Empty)?;
        if !matches!(entry.state.end, End::Open) {
            return Err(order::Front::Detached);
        }
        input::Target::new(&mut entry.owners, &mut entry.state.data, &mut entry.signal)
    }

    fn try_data(
        self,
        usage: D::Amount,
    ) -> Result<Option<slot::Push<'a, 'arena, 'credits, 'region, D>>, Reject> {
        Ok(self.input().map_err(Reject::Front)?.reserve(usage)?)
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
        input::build(builder, |amount| self.try_data(amount), slot::Push::commit)
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
