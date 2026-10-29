use std::cell::Cell;

use dope_core::driver::route::Identity;
use dope_net::link::pool;
use o3::depot::{
    arena::slab,
    array, capacity,
    queue::{links, tagged},
};

use crate::client::attempt::{
    self,
    queue::{
        self, Held, entry,
        storage::{self, Access, Linked, Occupied},
    },
};

/// Fixed attempt storage. Payload guards never escape a call or invoke user code.
pub(in crate::client::attempt) struct Table<'d, T: dope_net::Transport, ID: Identity> {
    pub(super) nodes: slab::Cell<storage::Node<'d, T, ID>, attempt::Tag<'d, ID>>,
    pub(super) heads: [links::Chain; 2],
    deferred: Cell<Option<attempt::Id<'d, ID>>>,
}

impl<'d, T: dope_net::Transport, ID: Identity> Table<'d, T, ID> {
    pub(super) fn try_new(max: capacity::Count) -> Result<Self, tagged::Error> {
        let shape = tagged::Domain::<[links::Chain; 2]>::try_from(max)?;
        Ok(Self {
            nodes: slab::Cell::try_new(shape.capacity())?,
            heads: [const { links::Chain::new() }; 2],
            deferred: Cell::new(None),
        })
    }

    fn clear(&self, key: attempt::Id<'d, ID>) {
        if self.deferred.get() == Some(key) {
            self.deferred.set(None);
        }
    }

    pub(super) fn free(&self, key: attempt::Id<'d, ID>) -> Result<(), queue::Lookup<'d, ID>> {
        match self.try_slot(key)?.access() {
            Access::Owned(slot) => slot.remove(),
            Access::Borrowed(slot) => slot.free(),
        }
        self.clear(key);
        Ok(())
    }

    pub(super) fn fail(
        &self,
        key: attempt::Id<'d, ID>,
    ) -> Result<attempt::Transition, queue::Lookup<'d, ID>> {
        Ok(self.try_slot(key)?.fail())
    }

    fn try_slot(
        &self,
        key: attempt::Id<'d, ID>,
    ) -> Result<Occupied<'_, 'd, T, ID>, queue::Lookup<'d, ID>> {
        let entry = self.nodes.try_entry(key.handle.handle()).map_err(|cause| {
            let (_, observed) = cause.into_parts();
            queue::Lookup::Key(storage::Error {
                key: key.handle,
                observed,
            })
        })?;
        Ok(Occupied::new(self, entry, key))
    }

    pub(super) fn finish(proof: Held<'_, 'd, T, ID>, commit: bool) {
        let table = *proof.source();
        let key = *proof.key();
        let (slot, change) = Occupied::finish(proof, commit);
        match change {
            entry::Change::Keep => {}
            entry::Change::Remove => {
                slot.remove();
                table.clear(key);
            }
            entry::Change::Queue => {
                tagged::Vacant::detach(slot)
                    .select(array::Index::at::<1>())
                    .push::<false>();
            }
        }
    }

    pub(super) fn poll_cancel(&self) -> Option<(attempt::Id<'d, ID>, pool::Key<'d, ID>)> {
        Some(Linked::<_, _, 1>::front(self)?.cancel())
    }

    pub(super) fn poll(&self) -> attempt::Action<'d, T, ID> {
        let Some(slot) = Linked::front(self) else {
            return attempt::Action::Idle;
        };
        let key = slot.key();
        self.clear(key);
        let plan = slot.open();
        attempt::Action::Connect { key, plan }
    }

    pub(super) fn connected(
        &self,
        key: attempt::Id<'d, ID>,
    ) -> Result<attempt::Transition, queue::Lookup<'d, ID>> {
        Ok(self.try_slot(key)?.connected())
    }

    pub(super) fn defer(
        &self,
        key: attempt::Id<'d, ID>,
        plan: attempt::Plan<T>,
    ) -> Result<(), (queue::Lookup<'d, ID>, attempt::Plan<T>)> {
        let slot = match self.try_slot(key) {
            Ok(slot) => slot,
            Err(cause) => return Err((cause, plan)),
        };
        let slot = match slot.access() {
            Access::Owned(_) => return Err((queue::Lookup::Finished(key), plan)),
            Access::Borrowed(slot) => slot,
        };
        slot.defer(plan, &self.deferred);
        Ok(())
    }

    pub(super) fn reject(&self, key: attempt::Id<'d, ID>) -> Result<(), queue::Lookup<'d, ID>> {
        self.try_slot(key)?.reject();
        Ok(())
    }

    pub(super) fn bind(
        &self,
        key: attempt::Id<'d, ID>,
        link: pool::Key<'d, ID>,
    ) -> Result<(), queue::Lookup<'d, ID>> {
        self.try_slot(key)?.bind(link);
        Ok(())
    }
}
