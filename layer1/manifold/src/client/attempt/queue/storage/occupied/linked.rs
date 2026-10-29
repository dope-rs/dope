use dope_core::driver::route::Identity;
use dope_net::link::pool;
use o3::{
    base::num::bounded,
    depot::{self, queue::tagged},
};

use crate::client::attempt::{
    self,
    queue::{
        storage::{self, Occupied},
        table::Table,
    },
};

/// Live membership carries the head proof through payload admission and unlink.
pub(in crate::client::attempt::queue) struct Linked<
    'a,
    'd,
    T: dope_net::Transport,
    ID: Identity,
    const L: usize,
>(tagged::Linked<Occupied<'a, 'd, T, ID>>);

impl<'a, 'd, T: dope_net::Transport, ID: Identity, const L: usize> Linked<'a, 'd, T, ID, L> {
    pub(in crate::client::attempt::queue) fn front(table: &'a Table<'d, T, ID>) -> Option<Self> {
        let lane = tagged::Endpoint::constant::<L>(table);
        let front = lane.front()?;
        // SAFETY: the endpoint retains its exact owner's initialized head.
        let front = unsafe { front.map(|(lane, index)| (lane.into_store(), index)) };
        let (table, index) = front.into_inner();
        // SAFETY: the consumed head proof retains this exact initialized front.
        let (entry, table) = unsafe { storage::admit(table, index) };
        let key = attempt::Id {
            handle: storage::Key::new(
                bounded::U32::from_one(index.position()),
                entry.key().generation(),
            ),
        };
        // SAFETY: the consumed head proof preserves live membership through admission.
        Some(Self(unsafe {
            depot::raw::TagEntry::linked(Occupied::new(table, entry, key))
        }))
    }
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> Linked<'a, 'd, T, ID, 0> {
    pub(in crate::client::attempt::queue) fn key(&self) -> attempt::Id<'d, ID> {
        self.0.entry().key
    }

    pub(in crate::client::attempt::queue) fn open(mut self) -> attempt::Plan<T> {
        // SAFETY: the linked owner retains exclusive access to its disjoint payload.
        let slot = unsafe { depot::raw::TagEntry::linked_mut(&mut self.0) };
        // SAFETY: only publication and defer insert into lane 0, both after
        // storing Queued. Consuming membership retains that exact payload.
        let plan = unsafe { slot.state().open() };
        self.0.unlink();
        plan
    }
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> Linked<'a, 'd, T, ID, 1> {
    pub(in crate::client::attempt::queue) fn cancel(
        mut self,
    ) -> (attempt::Id<'d, ID>, pool::Key<'d, ID>) {
        // SAFETY: the linked owner retains exclusive access to its disjoint payload.
        let slot = unsafe { depot::raw::TagEntry::linked_mut(&mut self.0) };
        // SAFETY: consuming lane 1 membership retains the Cancelled binding;
        // finish is its sole producer and has already consumed the lease.
        let binding = unsafe { slot.state().cancel() };
        let key = slot.key;
        self.0.unlink().into_inner().entry.remove();
        (key, binding)
    }
}
