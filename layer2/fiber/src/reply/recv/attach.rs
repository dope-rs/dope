//! Reply attachment admission.

use core::{mem, num::NonZero};

use o3::{base::cell::region, depot::capacity, mem::credit::fair};

use crate::reply::{Lane, LaneIndex, layout, order, usage};

/// Exact reason a reply owner could not attach to an arena lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum Error {
    IdentityExhausted,
    Payload(usage::Capacity),
    Oversized {
        requested: NonZero<usize>,
        capacity: layout::Entries,
    },
    Attached {
        current: LaneIndex,
        requested: LaneIndex,
    },
    Full {
        len: capacity::Count,
        capacity: layout::Entries,
    },
    Capacity {
        lane: LaneIndex,
        cause: fair::Cause<usize, 2>,
    },
}

/// Attaches a response owner to one arena lane.
pub trait Attach<'s, 'arena, 'credits, 'region, D, E = D, K = ()> {
    fn try_ordered(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<order::Depth, Error>;

    fn try_tagged(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<(), Error>;

    fn try_attach(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
    ) -> Result<(), Error>
    where
        K: Default,
    {
        self.try_tagged(token, lane, K::default())
    }

    fn detach(&mut self);
}

const _: () = assert!(mem::size_of::<Error>() == 5 * mem::size_of::<usize>());
