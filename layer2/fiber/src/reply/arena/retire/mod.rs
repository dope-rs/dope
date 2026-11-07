//! Incremental reply retirement evidence.

use std::{cell, mem};

use o3::{alloc, depot::capacity};

use crate::reply::arena::state;

mod cells;
pub(in crate::reply) use cells::Cell;

pub(in crate::reply) struct Retired {
    index: state::Index,
}

impl Retired {
    pub(in crate::reply) const fn new(index: state::Index) -> Self {
        Self { index }
    }

    pub(in crate::reply::arena) const fn index(&self) -> state::Index {
        self.index
    }
}

const _: () = assert!(mem::size_of::<Retired>() == mem::size_of::<u32>());

pub(in crate::reply::arena) struct State {
    queued: Cell<Retired>,
    pub(in crate::reply::arena) active: cell::Cell<Option<Retired>>,
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
