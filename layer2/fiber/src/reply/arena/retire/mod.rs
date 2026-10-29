//! Incremental reply retirement evidence.

use std::{cell, mem};

use o3::{alloc, depot::capacity};

use crate::reply::arena::state;

mod cells;
pub(in crate::reply) use cells::Cell;

pub(in crate::reply) struct Retired {
    key: state::EntryKey,
}

impl Retired {
    pub(in crate::reply) const fn new(key: state::EntryKey) -> Self {
        Self { key }
    }

    pub(in crate::reply::arena) const fn key(&self) -> state::EntryKey {
        self.key
    }
}

#[repr(transparent)]
pub(in crate::reply::arena) struct Active(Retired);

impl Active {
    pub(in crate::reply::arena) const fn new(retired: Retired) -> Self {
        Self(retired)
    }

    pub(in crate::reply::arena) const fn retired(&self) -> &Retired {
        &self.0
    }
}

const _: () = assert!(mem::size_of::<Active>() == mem::size_of::<Retired>());
const _: () = assert!(mem::size_of::<Option<Active>>() == mem::size_of::<Option<Retired>>());

pub(in crate::reply::arena) struct State {
    queued: Cell<Retired>,
    pub(in crate::reply::arena) active: cell::Cell<Option<Active>>,
}

impl State {
    pub(in crate::reply::arena) fn try_new(
        capacity: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            queued: Cell::try_new(capacity)?,
            active: cell::Cell::new(None),
        })
    }

    pub(in crate::reply::arena) fn push(&self, retired: Retired) {
        self.queued.push(retired);
    }

    pub(in crate::reply::arena) fn pop(&self) -> Option<Retired> {
        self.queued.pop_front()
    }

    pub(in crate::reply::arena) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub(in crate::reply::arena) fn has_active(&self) -> bool {
        let active = self.active.take();
        let present = active.is_some();
        self.active.set(active);
        present
    }
}
