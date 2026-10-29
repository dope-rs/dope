use dope_core::driver::route::{Identity, KeyTag};
use o3::{
    base::num::bounded,
    depot::{self, arena::slab, array},
};

use crate::client::attempt::{
    self,
    queue::{
        self, Held,
        entry::Entry,
        storage::{self, Node, Occupied},
        table::Table,
    },
};

/// Reserves one generation before target work; Drop retires the unpublished key.
#[must_use = "dropping an attempt reserve releases its generation"]
pub struct Reserve<'source, 'd, T: dope_net::Transport, ID: Identity = KeyTag<0>> {
    table: &'source Table<'d, T, ID>,
    free: slab::Reserved<'source, Node<'d, T, ID>, attempt::Tag<'d, ID>>,
}

impl<'source, 'd, T: dope_net::Transport, ID: Identity> Reserve<'source, 'd, T, ID> {
    pub(super) fn try_new(table: &'source Table<'d, T, ID>) -> Result<Self, slab::Vacancy> {
        Ok(Self {
            table,
            free: table.nodes.try_vacancy()?.reserve(),
        })
    }

    pub fn id(&self) -> attempt::Id<'d, ID> {
        attempt::Id {
            handle: storage::Key::new(
                bounded::U32::from_one(self.free.position()),
                self.free.key().generation(),
            ),
        }
    }

    /// Publishes the prepared plan while preserving the reserved generation.
    pub fn start(self, target: attempt::StreamTarget<T>) -> queue::Lease<'source, 'd, T, ID> {
        let key = self.id();
        let node = Node::new(
            key.handle.index().into(),
            Entry::queued(attempt::Plan::from_target(target)),
        );
        let entry = Occupied::new(self.table, self.free.occupy(node), key);
        // SAFETY: this consumed reservation initialized a detached node exactly once.
        unsafe { depot::raw::TagEntry::vacant(entry) }
            .select(array::Index::at::<0>())
            .push::<false>();
        // SAFETY: publication creates the sole lease for this initialized Queued
        // generation. Borrowed transitions retain a Pending phase until consumption.
        queue::Lease::new(unsafe { Held::new((self.table, key)) })
    }
}
