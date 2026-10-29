//! Owner-bound access to one reply resource lane.

use dope::core::driver::schedule::ready;
use o3::mem::{self, raw::Source as _};

use crate::reply::{
    self, LaneIndex,
    retain::{grant::payload, pool::Pool},
    usage,
};

const ENTRY: usize = 0;

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

    pub(in crate::reply) const fn new<'s, D, E, K>(
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
        A: mem::raw::Amount<2, usize>,
    {
        if self.pool.lanes().credits.can_admit(self.index, request) {
            reply::Registration::Available
        } else {
            reply::Registration::Full
        }
    }

    pub(in crate::reply) fn has_reserve(self) -> bool {
        let credit = self.pool.lanes().credits.capacity(self.index);
        credit.held()[ENTRY] < credit.reserve()[ENTRY]
    }

    pub(in crate::reply) fn try_reserve(
        self,
        usage: usage::Usage,
    ) -> Result<payload::Grant<'credits, payload::Failure>, usage::Capacity> {
        self.pool.lanes().try_reserve(self.index, usage)
    }

    pub(in crate::reply) fn release(self, grant: payload::Grant<'credits, payload::Failure>) {
        let lanes = self.pool.lanes();
        lanes.release_data(grant);
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
