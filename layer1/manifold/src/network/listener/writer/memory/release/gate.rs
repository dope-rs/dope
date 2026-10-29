use std::mem;

use dope_core::driver::{
    self,
    route::{Identity, KeyTag, table},
    schedule::ready,
};
use dope_net::link::pool;
use o3::{
    alloc,
    depot::{
        self,
        arena::pool::{Unique, recycle},
        queue::indexed::fifo::staged,
    },
};

use crate::network::listener::writer::memory;

pub(in crate::network::listener) struct Gate<'s, 'd, T, ID: Identity> {
    pub(super) permits: staged::Table<
        ready::Retained<'d>,
        (),
        Unique<'s, T, recycle::Header<T>>,
        pool::raw::Physical<'d, ID>,
    >,
    pub(super) store: &'s recycle::Store<T>,
    owner: driver::Reference<'d>,
}

impl<'s, 'd, T, ID: Identity> Gate<'s, 'd, T, ID> {
    pub(super) fn wake(&self, lease: Unique<'s, T, recycle::Header<T>>) {
        loop {
            // SAFETY: this core owns the queue; each front is consumed before the next access.
            let Some(entry) = staged::Slot::<_, _, _, staged::Queued, _>::first(unsafe {
                depot::raw::Proof::new(&self.permits)
            }) else {
                return;
            };
            let key = *entry.key();
            match self.owner.ready().try_activate(key) {
                Ok(()) => {
                    entry.set_parked(lease);
                    return;
                }
                Err(ready::Stale) => entry.clear(),
            }
        }
    }

    pub(in crate::network::listener::writer) fn try_new(
        store: &'s recycle::Store<T>,
        owner: driver::Reference<'d>,
        lanes: Option<table::Capacity>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            permits: match lanes {
                Some(capacity) => staged::Table::try_count(capacity),
                None => staged::Table::try_new(0),
            }?,
            store,
            owner,
        })
    }
}

const _: () = assert!(
    mem::size_of::<Gate<'static, 'static, memory::Header, KeyTag<0>>>()
        == 6 * mem::size_of::<usize>()
);
