use dope_core::driver::route::Identity;
use dope_net::link::pool;
use o3::depot::queue::tagged;

use crate::client::attempt::{
    self,
    queue::{
        Held,
        entry::{Change, Entry, Fail},
        storage,
        table::Table,
    },
};

pub(super) mod borrow;
pub(super) mod linked;

/// One admitted resident; only an owned capability may remove it.
pub(in crate::client::attempt::queue) struct Occupied<'a, 'd, T: dope_net::Transport, ID: Identity>
{
    pub(super) table: &'a Table<'d, T, ID>,
    pub(super) entry: storage::Resident<'a, 'd, T, ID>,
    pub(super) key: attempt::Id<'d, ID>,
}

impl<'a, 'd, T: dope_net::Transport, ID: Identity> Occupied<'a, 'd, T, ID> {
    pub(in crate::client::attempt::queue) fn new(
        table: &'a Table<'d, T, ID>,
        entry: storage::Resident<'a, 'd, T, ID>,
        key: attempt::Id<'d, ID>,
    ) -> Self {
        Self { table, entry, key }
    }

    pub(in crate::client::attempt::queue) fn connected(mut self) -> attempt::Transition {
        self.state().connected()
    }

    pub(in crate::client::attempt::queue) fn close(mut self) -> attempt::Transition {
        self.state().close()
    }

    pub(in crate::client::attempt::queue) fn reject(mut self) {
        self.state().reject();
    }

    pub(in crate::client::attempt::queue) fn bind(mut self, link: pool::Key<'d, ID>) {
        self.state().bind(link);
    }

    pub(in crate::client::attempt::queue) fn finish(
        proof: Held<'a, 'd, T, ID>,
        commit: bool,
    ) -> (Self, Change) {
        let (table, key) = proof.into_inner();
        // SAFETY: the consumed affine lease pins this exact initialized generation.
        // No payload guard crosses a callback or a public return.
        let (entry, table) = unsafe { storage::admit(table, key.handle.index().into()) };
        let mut slot = Self::new(table, entry, key);
        // SAFETY: only this consumed lease can leave the Pending phase domain.
        let change = unsafe { slot.state().finish(commit) };
        (slot, change)
    }

    pub(in crate::client::attempt::queue) fn free(mut self) {
        if self.state().is_owned() {
            self.remove();
        } else {
            self.state().done();
            tagged::Vacant::detach(self);
        }
    }

    pub(in crate::client::attempt::queue::storage) fn state(&mut self) -> &mut Entry<'d, T, ID> {
        storage::Node::state(&mut self.entry)
    }

    pub(in crate::client::attempt::queue) fn fail(mut self) -> attempt::Transition {
        match self.state().fail() {
            Fail::Keep => attempt::Transition::Applied,
            Fail::Remove => {
                self.remove();
                attempt::Transition::Stale
            }
            Fail::Stale => attempt::Transition::Stale,
        }
    }

    pub(in crate::client::attempt::queue) fn remove(self) {
        tagged::Vacant::detach(self).into_inner().entry.remove();
    }
}
