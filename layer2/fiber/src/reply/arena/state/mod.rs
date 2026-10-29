//! Reply arena state transitions.

use std::{mem, task};

use dope::core::driver::schedule::ready::wake::Waker;
use head::Head;
use keys::EntryTag;
use o3::{
    alloc,
    depot::{arena::slab, capacity, queue::round},
};

use crate::{
    reply,
    reply::{
        Attachment, LaneIndex, layout,
        order::Depth,
        recv,
        retain::{grant::entries, pool, pool::Pool},
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
pub(in crate::reply::arena) use register::Register;
pub(in crate::reply::arena) use retire::{Advance, Retire, Start};
pub(in crate::reply::arena) use terminal::{Completion, Failure, Terminal};

pub(super) struct Ordered<'credits, K = ()> {
    key: EntryKey,
    grant: entries::Grant<'credits, entries::Units<0, 1>>,
    tag: K,
}

impl<'credits, K> Ordered<'credits, K> {
    pub(super) const fn new(
        key: EntryKey,
        grant: entries::Grant<'credits, entries::Units<0, 1>>,
        tag: K,
    ) -> Self {
        Self { key, grant, tag }
    }

    fn take(self) -> entries::Grant<'credits, entries::Units<0, 1>> {
        self.grant
    }
}

pub(super) struct Entry<'arena, 'credits, 'region, D, E> {
    pub(super) body: EntryBody<'arena, D, E>,
    pub(super) acceptance: recv::Acceptance,
    pub(super) owners: pool::Owner<'arena, 'credits, 'region>,
    pub(super) signal: Signal<'region>,
}

pub(in crate::reply) struct Signal<'region> {
    notified: bool,
    wake: Option<Waker<'region>>,
}

impl<'region> Signal<'region> {
    pub(super) const fn new() -> Self {
        Self {
            notified: false,
            wake: None,
        }
    }

    pub(in crate::reply) fn notify(&mut self) {
        self.notified = true;
        if let Some(wake) = self.wake.take() {
            wake.wake();
        }
    }

    pub(in crate::reply) fn poll(&mut self, wake: Waker<'region>) -> task::Poll<()> {
        if self.notified {
            self.notified = false;
            self.wake = None;
            task::Poll::Ready(())
        } else {
            self.wake = Some(wake);
            task::Poll::Pending
        }
    }
}

pub(super) enum EntryBody<'arena, D, E> {
    Attached(Attached<reply::State<'arena, D, E>>),
    Draining(reply::State<'arena, D, E>),
    Drained,
}

#[repr(transparent)]
pub(super) struct Attached<S>(S);

#[derive(Default)]
pub(super) struct Lane {
    live: usize,
}

impl Lane {
    pub(in crate::reply::arena) const fn attachment(&self) -> Attachment {
        if self.live == 0 {
            Attachment::Detached
        } else {
            Attachment::Attached
        }
    }
}

impl<S> Attached<S> {
    pub(in crate::reply::arena) fn state(&mut self) -> &mut S {
        &mut self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Attached<[u8; 3]>>() == 3);
    assert!(mem::size_of::<Lane>() == mem::size_of::<usize>());
};

#[repr(transparent)]
pub(in crate::reply::arena) struct Entries<'arena, 'credits, 'region, D, E>(
    slab::Store<Entry<'arena, 'credits, 'region, D, E>, EntryTag>,
);

pub(in crate::reply::arena) struct Book<'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) slots: Entries<'arena, 'credits, 'region, D, E>,
    pub(in crate::reply::arena) order: order::Queues<'credits, K>,
    pub(in crate::reply::arena) limit: layout::Limit,
}

impl<'arena, 'credits, 'region, D, E, K> Book<'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) fn try_new(layout: layout::Layout) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: Entries::try_new(layout)?,
            order: order::Queues::try_new(layout)?,
            limit: layout.limit(),
        })
    }

    pub(in crate::reply::arena) fn front_tag(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Option<&K> {
        Some(&self.order.view(lane.index).front()?.tag)
    }

    pub(in crate::reply::arena) fn depth(
        &self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) -> Depth {
        Depth::from_order(self.order.view(lane.index).count())
    }

    pub(in crate::reply::arena) fn activate(
        &mut self,
        lane: reply::Lane<'_, 'arena, 'credits, 'region, D, E, K>,
    ) {
        use crate::reply::retain::pool::Lane;

        if self.order.insert::<0>(lane.index.into()) == round::Insertion::Present {
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
        for entry in slab::Slots::new(&mut self.slots.0).entries() {
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
                // SAFETY: both values were removed together from this exact
                // occupied entry and stay paired until every member is returned.
                let mut retire = unsafe { reply::raw::Retire::new(&mut state, &mut owners) };
                while let reply::Drain::Dropped = retire.advance() {}
            }
            owners.cancel_entry();
            owners.retire();
            owners.unbind();
        }
    }
}

impl<'arena, 'credits, 'region, D, E> Entries<'arena, 'credits, 'region, D, E> {
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

    pub(in crate::reply::arena) fn get_mut(
        &mut self,
        key: EntryKey,
    ) -> Option<&mut Entry<'arena, 'credits, 'region, D, E>> {
        self.0.get_mut(key.raw())
    }
}

const _: () = assert!(
    mem::size_of::<Entries<'static, 'static, 'static, (), ()>>()
        == mem::size_of::<slab::Store<Entry<'static, 'static, 'static, (), ()>, EntryTag>>()
);
