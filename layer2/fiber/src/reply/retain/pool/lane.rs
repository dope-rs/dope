//! Owner-bound access to one reply resource lane.

use dope::core::driver::schedule::ready;
use o3::mem;

use crate::reply::{
    self, LaneIndex,
    retain::{
        self,
        grant::payload,
        pool::{Acquire, Pool},
    },
    usage,
    usage::Footprint,
};

#[derive(Clone, Copy)]
pub(in crate::reply) struct Lane<'arena, 'credits, 'region: 'arena> {
    pool: &'arena Pool<'credits, 'region>,
    index: LaneIndex,
}

pub(in crate::reply) enum Release {
    Reserved,
    Shared,
}

impl<'arena, 'credits, 'region> Lane<'arena, 'credits, 'region> {
    pub(in crate::reply::retain::pool) const fn pool(self) -> &'arena Pool<'credits, 'region> {
        self.pool
    }

    pub(in crate::reply) const fn new<'s, D: Footprint, E, K>(
        lane: reply::Lane<'s, 'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        Self {
            pool: &lane.arena.backing.resources,
            index: lane.index,
        }
    }

    pub(in crate::reply) fn bind(self, target: ready::Target<'region>) {
        self.pool.lanes().bind(self.index, target);
    }

    pub(in crate::reply) const fn index(self) -> LaneIndex {
        self.index
    }

    pub(in crate::reply) fn registration<A>(self, request: A) -> reply::Registration
    where
        A: mem::raw::Amount<1, usize>,
    {
        if self.pool.lanes().credits.can_admit(self.index, request) {
            reply::Registration::Available
        } else {
            reply::Registration::Full
        }
    }

    pub(in crate::reply) fn has_reserve(self) -> bool {
        self.pool.lanes().credits.state(self.index).has_reserve()
    }

    pub(in crate::reply) fn try_reserve(
        self,
        usage: usage::Usage,
    ) -> Result<retain::Reservation<'arena, 'credits, 'region>, usage::Capacity> {
        let source = self.pool.lanes().credits.state(self.index);
        let amount = payload::Failure::new(usage);
        source
            .try_acquire(mem::raw::Amount::get(amount))
            .map_err(|cause| match Acquire::new(cause) {
                Acquire::Capacity(cause) => cause,
                Acquire::Backpressure(cause) => usage::Capacity::Unavailable(cause),
            })?;
        Ok(retain::Reservation {
            lane: self,
            source,
            amount,
        })
    }

    /// # Safety
    /// The original Reservation consumes its unchanged source and amount once.
    pub(in crate::reply::retain) unsafe fn release(
        self,
        source: &retain::raw::State,
        amount: payload::Failure,
    ) {
        let lanes = self.pool.lanes();
        unsafe { lanes.release_data(source, amount) };
        if lanes.has_pending() {
            lanes.wake(self.index);
        }
    }

    pub(in crate::reply) fn wake(self) {
        self.pool.lanes().wake(self.index);
    }

    pub(in crate::reply) fn cancel(self) {
        self.pool.lanes().cancel(self.index);
    }
}

const _: () = assert!(
    std::mem::size_of::<Lane<'static, 'static, 'static>>() == 2 * std::mem::size_of::<usize>()
);
