use std::{marker, mem, num::NonZero};

use dope::{
    core::driver::{
        route::Identity,
        schedule::{app, ready},
    },
    manifold::client::conn,
};
use o3::{
    alloc::array,
    base::{cell::region, num::bounded},
    depot,
    depot::queue::links,
    mem::quota,
};

use crate::reply::{
    Arena, Progress, Work,
    arena::{
        control::Control,
        retire::Retired,
        state,
        state::{Completion, Failure, Push, Register, Terminal},
    },
    layout::Domain,
    order, recv,
    recv::attach,
    retain::{self, pool},
    usage,
};

/// Identity of one lane within a reply arena layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct LaneIndex(bounded::U32<0, { u32::MAX - 2 }>);

impl LaneIndex {
    /// Creates a lane identity for a later arena-capacity check.
    pub const fn new<const INDEX: u32>() -> Self {
        Self(bounded::U32::exact::<INDEX>())
    }

    pub const ZERO: Self = Self(bounded::U32::exact::<0>());

    pub(in crate::reply) fn from_conn<ID: Identity>(lane: conn::Lane<'_, ID>) -> Self {
        let index = lane.slot().position().get() - 1;
        // SAFETY: connection slot indices are bounded by the 24-bit route domain.
        unsafe { depot::raw::Compact::from_raw(index) }
    }

    pub(in crate::reply) const fn try_within(
        index: Self,
        lanes: NonZero<u32>,
    ) -> Result<Self, Error> {
        if index.get() >= lanes.get() as usize {
            return Err(Error::Lane {
                index,
                capacity: lanes,
            });
        }
        Ok(index)
    }

    pub const fn get(self) -> usize {
        self.0.into_usize()
    }
}

impl From<LaneIndex> for links::Index {
    fn from(index: LaneIndex) -> Self {
        Self::from(index.0)
    }
}

impl From<bounded::U32<0, { u32::MAX - 2 }>> for LaneIndex {
    fn from(index: bounded::U32<0, { u32::MAX - 2 }>) -> Self {
        Self(index)
    }
}

const _: () = assert!(mem::size_of::<LaneIndex>() == mem::size_of::<u32>());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Lane {
        index: LaneIndex,
        capacity: NonZero<u32>,
    },
}

const _: () = assert!(mem::size_of::<Error>() == 2 * mem::size_of::<u32>());

/// A reply-arena lane branded so it cannot escape its driver-owned arena.
pub struct Lane<'a, 'arena, 'credits, 'region: 'a, D, E = D, K = ()> {
    pub(in crate::reply) arena: &'a Arena<'arena, 'credits, 'region, D, E, K>,
    pub(in crate::reply) index: LaneIndex,
}

impl<D, E, K> Copy for Lane<'_, '_, '_, '_, D, E, K> {}

impl<D, E, K> Clone for Lane<'_, '_, '_, '_, D, E, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Lane<'a, 'arena, 'credits, 'region, D, E, K> {
    pub const fn index(self) -> LaneIndex {
        self.index
    }

    pub const fn control(self) -> Control<'a, 'arena, 'credits, 'region, D, E, K> {
        Control::new(self)
    }

    /// Binds maintenance to this lane's current driver task.
    pub fn bind_retire(self, target: ready::Target<'region>) {
        pool::Lane::new(self).bind(target);
        if matches!(self.arena.work(), Work::Pending) {
            target.wake();
        }
    }

    pub(in crate::reply) fn defer_retire(self, retired: Retired) {
        self.arena.retire.push(retired);
        pool::Lane::new(self).wake();
    }

    pub fn depth(self, token: &mut region::Token<region::Identity<'region>>) -> order::Depth {
        token.borrow(&self.arena.state).depth(self)
    }

    /// Retains this exact ordered front until its inspection or insertion ends.
    pub fn select<'p>(
        self,
        token: &'p mut region::Token<region::Identity<'region>>,
    ) -> Push<'p, 'arena, 'credits, 'region, D, E, K>
    where
        'a: 'p,
    {
        Push::new(token.borrow_mut(&self.arena.state), self)
    }

    /// Reserves byte and protocol-value credit without installing a reply waiter.
    /// The returned owner keeps this capacity unavailable to arriving data.
    pub fn try_reserve(
        self,
        usage: usage::Usage,
    ) -> Result<retain::Reservation<'arena, 'credits, 'region>, usage::Capacity> {
        retain::Reservation::try_new(pool::Lane::new(self), usage)
    }

    pub fn complete(self, token: &mut region::Token<region::Identity<'region>>) -> order::Depth {
        let done = Terminal::new(self, token.borrow_mut(&self.arena.state)).complete();
        match done {
            Completion::Complete(depth) => depth,
            Completion::Drained { depth, retired } => {
                self.defer_retire(retired);
                depth
            }
        }
    }

    /// Stores a zero-resource terminal value inline when an attached reply is
    /// currently at the front. Otherwise the cause is consumed as a no-op.
    pub fn fail_one<C>(
        self,
        token: &mut region::Token<region::Identity<'region>>,
        cause: C,
    ) -> order::Depth
    where
        E: for<'cause> From<&'cause C>,
    {
        let failed = Terminal::new(self, token.borrow_mut(&self.arena.state)).fail_one(cause);
        match failed {
            Some(depth) => depth,
            None => token.borrow(&self.arena.state).depth(self),
        }
    }
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Lane<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply) fn register(
        self,
        token: &mut region::Token<region::Identity<'region>>,
        accept: recv::Acceptance,
        tag: K,
    ) -> Result<state::EntryKey, attach::Error> {
        Register::new(token.borrow_mut(&self.arena.state)).insert(
            self,
            accept,
            tag,
            self.arena.backing.items.fifo(),
        )
    }

    pub(in crate::reply) fn group_with<I, R, const N: usize>(
        self,
        token: &mut region::Token<region::Identity<'region>>,
        accept: recv::Acceptance,
        mode: recv::Acceptance,
        input: I,
        make: impl FnOnce(I) -> ([K; N], K, R),
    ) -> Result<([state::EntryKey; N], state::EntryKey, R), (attach::Error, I)> {
        Register::new(token.borrow_mut(&self.arena.state))
            .group_with(self, accept, mode, input, make)
    }
}

/// Exclusive region access restricted to failing one reply lane.
#[repr(transparent)]
pub struct Retirement<'a, 'region> {
    token: &'a mut region::Token<region::Identity<'region>>,
}

impl<'a, 'region> Retirement<'a, 'region> {
    pub const fn new(token: &'a mut region::Token<region::Identity<'region>>) -> Self {
        Self { token }
    }

    /// Borrows the cause so its owner can retain it across scheduling turns.
    pub fn fail<'s, 'arena, 'credits, D, E, K, C>(
        self,
        work: app::Work<'_, 'region>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        cause: &C,
    ) -> Progress
    where
        E: for<'cause> From<&'cause C>,
    {
        let token = self.token;
        loop {
            let admitted = work.run(|| {
                let failure = Terminal::new(lane, token.borrow_mut(&lane.arena.state)).fail(cause);
                match failure {
                    Failure::Empty => None,
                    Failure::Advanced => Some(()),
                    Failure::Drained(retired) => {
                        lane.defer_retire(retired);
                        Some(())
                    }
                }
            });
            return match admitted {
                quota::Admission::Item(()) => continue,
                quota::Admission::Empty => Progress::Complete(order::Depth::ZERO),
                quota::Admission::Exhausted => {
                    let depth = token.borrow(&lane.arena.state).depth(lane);
                    if depth == order::Depth::ZERO {
                        Progress::Complete(depth)
                    } else {
                        Progress::Pending(depth)
                    }
                }
            };
        }
    }
}

const _: () = assert!(
    mem::size_of::<Retirement<'static, 'static>>()
        == mem::size_of::<&mut region::Token<region::Identity<'static>>>()
);

/// The exact arena lane whose entry credit was physically reclaimed.
#[must_use = "released lane evidence must be consumed"]
#[repr(transparent)]
pub struct Released<'arena>(
    NonZero<usize>,
    marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
);

impl<'arena> Released<'arena> {
    pub(in crate::reply::arena) const fn new(index: LaneIndex, _: Domain<'arena>) -> Self {
        Self(
            NonZero::<usize>::MIN.saturating_add(index.get()),
            marker::PhantomData,
        )
    }

    pub fn index(self) -> LaneIndex {
        // SAFETY: this decodes the exact bounded lane retained by Released::new.
        unsafe { depot::raw::Compact::from_raw((self.0.get() - 1) as u32) }
    }

    /// Projects the released lane into initialized storage of the same arena domain.
    pub fn entry<'a, T>(self, entries: &'a array::Block<T, Domain<'arena>>) -> &'a T {
        // SAFETY: both the released lane and the allocation retain the invariant
        // arena brand. Its domain has one immutable extent and all entries are initialized.
        unsafe { entries.get_unchecked(self.index().get()) }
    }
}

const _: () = {
    assert!(mem::size_of::<Released<'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Released<'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Option<Released<'static>>>() == mem::size_of::<usize>());
};
