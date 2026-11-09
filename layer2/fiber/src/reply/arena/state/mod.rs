//! Reply arena state transitions.

use std::{marker, mem};

use dope::core::driver::schedule::ready::wake::Waker;
use head::Head;
use keys::EntryTag;
use o3::{
    alloc,
    base::cell::region,
    depot::{self, arena::slab, capacity, index::Initialized},
};

use crate::{
    reply,
    reply::{
        LaneIndex, layout,
        order::Depth,
        retain::{pool, pool::Pool},
        usage::Footprint,
    },
};

mod head;
mod keys;
pub(in crate::reply) mod order;
mod push;
mod register;
mod retire;
mod terminal;

pub(in crate::reply) use keys::{EntryKey, Index};
pub use push::Push;
pub(in crate::reply::arena) use register::Ordered;
pub(in crate::reply::arena) use retire::Advance;
pub(in crate::reply::arena) use terminal::Failure;

pub(in crate::reply) type Registered<'s, A, P> =
    depot::raw::Proof<(&'s A, EntryKey), Initialized<P>>;

pub(super) struct Entry<'arena, 'credits, 'region, D: Footprint, E> {
    pub(super) body: EntryBody<'arena, D, E>,
    pub(super) owners: pool::Owner<'arena, 'credits, 'region>,
    pub(super) signal: Signal<'region>,
}

pub(in crate::reply) struct Signal<'region> {
    wake: Option<Waker<'region>>,
}

impl<'region> Signal<'region> {
    pub(super) const fn new() -> Self {
        Self { wake: None }
    }

    pub(in crate::reply) fn notify(&mut self) {
        if let Some(wake) = self.wake.take() {
            wake.wake();
        }
    }

    pub(in crate::reply) fn register(&mut self, wake: Waker<'region>) {
        self.wake = Some(wake);
    }
}

pub(super) enum EntryBody<'arena, D: Footprint, E> {
    Attached(Attached<reply::State<'arena, D, E>>),
    Draining(reply::State<'arena, D, E>),
    Drained,
}

#[repr(transparent)]
pub(super) struct Attached<S>(S);

impl<S> Attached<S> {
    pub(in crate::reply::arena) fn state(&mut self) -> &mut S {
        &mut self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Attached<[u8; 3]>>() == 3);
};

#[repr(transparent)]
pub(in crate::reply::arena) struct Entries<'arena, 'credits, 'region, D: Footprint, E>(
    slab::Store<Entry<'arena, 'credits, 'region, D, E>, EntryTag>,
);

pub(in crate::reply::arena) struct Book<'arena, 'credits, 'region, D: Footprint, E, K> {
    pub(in crate::reply::arena) slots: Entries<'arena, 'credits, 'region, D, E>,
    pub(in crate::reply::arena) order: order::Queues<K>,
}

impl<'arena, 'credits, 'region, D: Footprint, E, K>
    reply::Arena<'arena, 'credits, 'region, D, E, K>
{
    pub(in crate::reply) fn slot<'a, 's: 'a, P: 'a>(
        token: &'a mut region::Token<region::Identity<'region>>,
        registered: Registered<'s, Self, P>,
    ) -> Option<(
        reply::Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
        &'a mut Signal<'region>,
    )> {
        // SAFETY: the registration and region loan retain the exact occupied slot.
        let live = unsafe {
            registered.map(|(arena, key)| {
                (
                    (&mut token.borrow_mut(&arena.state).slots.0, ()),
                    key.index().into(),
                )
            })
        };
        let (entry, ()) = slab::Occupied::from_live(live);
        let entry = entry.into_mut();
        let EntryBody::Attached(slot) = &mut entry.body else {
            return None;
        };
        Some((
            reply::Slot {
                state: slot.state(),
                owners: &mut entry.owners,
                owner: marker::PhantomData,
            },
            &mut entry.signal,
        ))
    }

    pub(in crate::reply) fn registered_order<P>(
        token: &region::Token<region::Identity<'region>>,
        registered: Registered<'_, Self, P>,
    ) -> reply::order::State {
        // SAFETY: the retained arena owns this exact initialized entry; the
        // region loan keeps its owner unchanged throughout the projection.
        let live = unsafe {
            registered.map(|(arena, key)| (&token.borrow(&arena.state).slots.0, key.index().into()))
        };
        slab::Store::value(live).source().owners.order()
    }
}

impl<'arena, 'credits, 'region, D: Footprint, E, K> Book<'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) fn try_new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: Entries::try_new(layout)?,
            order: order::Queues::try_new(layout)?,
        })
    }

    pub(in crate::reply::arena) fn front_tag(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Option<&K> {
        Some(self.order.view(lane.index).front()?.tag())
    }

    pub(in crate::reply::arena) fn depth(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Depth {
        Depth::from_order(self.order.view(lane.index).count())
    }

    pub(in crate::reply::arena) fn cutoff(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> reply::order::Cutoff<'arena> {
        reply::order::Cutoff::new(
            self.order.view(lane.index).back().map(Ordered::key),
            lane.arena.domain(),
        )
    }

    pub(in crate::reply::arena) fn contains_cutoff(
        &self,
        lane: LaneIndex,
        cutoff: reply::order::Cutoff<'arena>,
    ) -> bool {
        let Some(end) = cutoff.key() else {
            return false;
        };
        self.slots.0.try_get(end.raw()).is_ok_and(|entry| {
            entry.owners.order() == reply::order::State::Linked && entry.owners.lane() == lane
        })
    }

    pub(in crate::reply::arena) fn activate(
        &mut self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) {
        use crate::reply::retain::pool::Lane;

        if !self.order.insert::<0>(lane.index.into()) {
            return;
        }
        let credit = Lane::new(lane);
        if credit.has_reserve() {
            self.order.reserve(credit.index());
        }
    }

    pub(in crate::reply::arena) fn pick_active(
        &mut self,
        credit: &Pool<'credits, 'region>,
    ) -> Option<LaneIndex> {
        if credit.shared() != 0 {
            self.order.next::<0>()
        } else {
            self.order.next::<1>()
        }
    }

    pub(in crate::reply::arena) fn clear(&mut self) {
        self.order.clear();
        for entry in self.slots.0.entries() {
            let slab::Entry::Occupied(entry) = entry else {
                continue;
            };
            let Entry {
                body, mut owners, ..
            } = entry.remove();
            owners.clear_order();
            let state = match body {
                EntryBody::Attached(attached) => Some(attached.0),
                EntryBody::Draining(state) => Some(state),
                EntryBody::Drained => None,
            };
            if let Some(mut state) = state {
                let mut slot = reply::Slot {
                    state: &mut state,
                    owners: &mut owners,
                    owner: marker::PhantomData,
                };
                while slot.drop_next() {}
            }
            owners.cancel_entry();
            owners.retire();
            owners.unbind();
        }
    }
}

impl<'arena, 'credits, 'region, D: Footprint, E> Entries<'arena, 'credits, 'region, D, E> {
    fn try_new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        let count = capacity::Count::new(layout.entries().get());
        Ok(Self(slab::Store::<
            Entry<'arena, 'credits, 'region, D, E>,
            EntryTag,
        >::try_new(count)?))
    }

    pub(in crate::reply::arena) fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub(in crate::reply::arena) fn is_full(&self) -> bool {
        self.0.is_full()
    }

    pub(in crate::reply::arena) fn is_exhausted(&self) -> bool {
        self.0.remaining().get() == 0
    }
}

const _: () = assert!(
    mem::size_of::<Entries<'static, 'static, 'static, (), ()>>()
        == mem::size_of::<slab::Store<Entry<'static, 'static, 'static, (), ()>, EntryTag>>()
);
