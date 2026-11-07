//! Mutable protocol order transitions.

use std::{array, mem::size_of};

use o3::{
    alloc, depot,
    depot::{
        capacity,
        queue::{links, pool},
    },
    mem,
};

use crate::reply::{
    LaneIndex,
    arena::{retire::Retired, state},
    layout, order, recv,
    retain::{
        grant::payload,
        pool::{Acquire, Owner},
    },
};

mod lease;

pub(in crate::reply) use lease::Lease;

pub(in crate::reply) struct Slot<'a, 'arena, 'credits, 'region, K = ()> {
    front: Front<'a, K>,
    pub(super) key: state::EntryKey,
    pub(super) owner: &'a mut Owner<'arena, 'credits, 'region>,
}

impl<'a, 'arena, 'credits, 'region, K> Slot<'a, 'arena, 'credits, 'region, K> {
    pub(super) const fn new(
        front: Front<'a, K>,
        key: state::EntryKey,
        owner: &'a mut Owner<'arena, 'credits, 'region>,
    ) -> Self {
        Self { front, key, owner }
    }

    pub(in crate::reply) fn try_lease<A>(
        self,
        amount: A,
        limit: layout::Limit,
    ) -> Result<Lease<'a, 'arena, 'credits, 'region, A, K>, Acquire>
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Requested>,
    {
        let acceptance = self.owner.acceptance.unwrap_or(recv::Acceptance::Collected);
        self.owner.payload.try_acquire(amount, limit, acceptance)?;
        Ok(Lease::new(self, amount))
    }

    pub(in crate::reply) const fn acceptance(&self) -> Option<recv::Acceptance> {
        self.owner.acceptance
    }

    pub(in crate::reply) fn cancel_pop(self) -> order::Depth {
        self.owner.cancel_entry();
        order::Depth::from_order(self.take().0)
    }

    pub(super) fn reclaim(self) -> order::Depth {
        let (count, _) = self.take();
        order::Depth::from_order(count)
    }

    /// Pops the order before admitting its sole retirement owner.
    pub(super) fn retire(self) -> (order::Depth, Option<Retired>) {
        let key = self.key;
        let (count, owner) = self.take();
        let retired = owner.is_empty().then(|| Retired::new(key.index()));
        (order::Depth::from_order(count), retired)
    }

    pub(in crate::reply::arena::state) fn take(
        self,
    ) -> (capacity::Count, &'a mut Owner<'arena, 'credits, 'region>) {
        let (ordered, count) = self.front.pop();
        drop(ordered);
        self.owner.clear_order();
        (count, self.owner)
    }
}

struct Lane {
    state: state::Lane,
    links: [links::Hook; 2],
}

pub(in crate::reply::arena) struct Queues<K = ()> {
    lanes: pool::Lanes<state::Ordered<K>, Lane, false>,
    heads: [links::Cycle; 2],
}

pub(in crate::reply::arena) struct View<'a, K>(pool::View<'a, state::Ordered<K>, Lane, false>);

pub(in crate::reply::arena) struct Route<'a, K>(pool::Lane<'a, state::Ordered<K>, Lane, false>);

pub(in crate::reply::arena) struct Front<'a, K = ()>(
    pool::Front<'a, state::Ordered<K>, Lane, false>,
);

struct Hooks<'a, K, const N: usize>(&'a pool::Lanes<state::Ordered<K>, Lane, false>);

struct Insert<'a, K> {
    lane: pool::Lane<'a, state::Ordered<K>, Lane, false>,
    item: state::Ordered<K>,
}

fn lane(index: links::Index) -> LaneIndex {
    // SAFETY: every member was initialized within this layout's lane domain.
    unsafe { depot::raw::Compact::from_raw(index.raw()) }
}

// SAFETY: each indexed lane retains two stable, disjoint hook families.
unsafe impl<K, const N: usize> depot::raw::Hooks for Hooks<'_, K, N> {
    unsafe fn hook_raw(&self, index: links::Index) -> &links::Hook {
        const { assert!(N < 2) };
        // SAFETY: the caller retains this exact initialized lane index.
        &unsafe { depot::raw::Project::project(self.0, index.get()) }
            .state()
            .links[N]
    }
}

// SAFETY: only Register::publish creates Ordered, after consuming a distinct
// entry vacancy and its admission. Each order retains that occupied entry until
// removal; equal entry and queue capacities therefore guarantee this slot.
unsafe impl<'a, K: 'a> depot::raw::PoolInsert<'a, false> for Insert<'a, K> {
    type Item = state::Ordered<K>;
    type State = Lane;

    unsafe fn into_parts(self) -> (pool::Lane<'a, Self::Item, Self::State, false>, Self::Item) {
        (self.lane, self.item)
    }
}

impl<K> Queues<K> {
    /// Each order retains a distinct occupied entry until it is popped;
    /// retirement requires the owner to have cleared that order first.
    pub(in crate::reply::arena) fn try_new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        let plan = pool::Plan::from_counts(
            capacity::Count::new(layout.entries().get()),
            capacity::Count::new(layout.lane_count().get()),
        );
        let lanes = pool::Lanes::try_from_fn(plan, |index| Lane {
            state: state::Lane::default(),
            links: array::from_fn(|_| links::Hook::new(index)),
        })?;
        Ok(Self {
            lanes,
            heads: array::from_fn(|_| links::Cycle::new()),
        })
    }

    pub(in crate::reply::arena) fn clear(&mut self) {
        for mut lane in self.lanes.iter_mut() {
            while lane.pop_front().is_some() {}
        }
    }

    pub(in crate::reply::arena) fn view(&self, index: LaneIndex) -> View<'_, K> {
        // SAFETY: the arena binds public lanes and this store to the same layout.
        View(unsafe { depot::raw::Project::project(&self.lanes, index.get()) })
    }

    pub(in crate::reply::arena) fn route(&mut self, index: LaneIndex) -> Route<'_, K> {
        // SAFETY: the arena binds public lanes and this store to the same layout.
        Route(unsafe { depot::raw::Project::project(&mut self.lanes, index.get()) })
    }

    pub(in crate::reply::arena) fn deactivate(&mut self, lane: LaneIndex) {
        let member = lane.into();
        self.remove::<0>(member);
        self.remove::<1>(member);
    }

    pub(in crate::reply::arena) fn contains(&self, lane: LaneIndex) -> bool {
        self.link::<0>(lane.into()).is_linked(lane.get())
    }

    pub(in crate::reply::arena) fn reserve(&mut self, lane: LaneIndex) {
        self.insert::<1>(lane.into());
    }

    pub(in crate::reply::arena) fn next<const N: usize>(&mut self) -> Option<LaneIndex> {
        const { assert!(N < 2) };
        let head = self.heads[N].first()?;
        // SAFETY: only this exact hook family enters the reciprocal ring.
        unsafe { depot::raw::Hooks::cycle_step(&Hooks::<K, N>(&self.lanes), &self.heads[N], head) };
        Some(lane(head))
    }

    fn link<const N: usize>(&self, member: links::Index) -> &links::Hook {
        const { assert!(N < 2) };
        // SAFETY: every member belongs to this immutable initialized lane domain.
        &unsafe { depot::raw::Project::project(&self.lanes, member.get()) }
            .state()
            .links[N]
    }

    pub(in crate::reply::arena) fn insert<const N: usize>(&self, member: links::Index) -> bool {
        if self.link::<N>(member).is_linked(member.get()) {
            return false;
        }
        // SAFETY: this exact hook is vacant and its family's ring is reciprocal.
        unsafe {
            depot::raw::Hooks::cycle_push(&Hooks::<K, N>(&self.lanes), &self.heads[N], member)
        };
        true
    }

    pub(in crate::reply::arena) fn remove<const N: usize>(&self, member: links::Index) {
        if self.link::<N>(member).is_linked(member.get()) {
            // SAFETY: membership belongs exclusively to this hook family's ring.
            unsafe {
                depot::raw::Hooks::cycle_unlink(&Hooks::<K, N>(&self.lanes), &self.heads[N], member)
            };
        }
    }
}

impl<'a, K> View<'a, K> {
    pub(in crate::reply::arena) fn count(&self) -> capacity::Count {
        self.0.count()
    }

    pub(in crate::reply::arena) fn state(&self) -> &'a state::Lane {
        &self.0.state().state
    }

    pub(in crate::reply::arena) fn front(&self) -> Option<&'a state::Ordered<K>> {
        self.0.front()
    }

    pub(in crate::reply::arena) fn back(&self) -> Option<&'a state::Ordered<K>> {
        self.0.back()
    }
}

impl<'a, K> Route<'a, K> {
    pub(in crate::reply::arena) fn into_state(self) -> &'a mut state::Lane {
        &mut self.0.into_state().state
    }

    pub(in crate::reply::arena) fn front(self) -> Option<Front<'a, K>> {
        self.0.front_entry().map(Front)
    }

    pub(in crate::reply::arena) fn push(self, item: state::Ordered<K>) {
        pool::Lane::insert(Insert { lane: self.0, item });
    }
}

impl<K> Front<'_, K> {
    pub(in crate::reply::arena) fn value(&mut self) -> &state::Ordered<K> {
        self.0.get_mut()
    }

    pub(in crate::reply::arena) fn pop(self) -> (state::Ordered<K>, capacity::Count) {
        let (ordered, lane) = self.0.take();
        (ordered, lane.count())
    }
}

const _: () = {
    assert!(size_of::<Queues>() == 6 * size_of::<usize>());
    assert!(size_of::<Lane>() == 3 * size_of::<usize>());
    assert!(size_of::<Front<'static>>() == 3 * size_of::<usize>());
};
