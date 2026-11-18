//! Incremental reply retirement evidence.

use std::mem;

use o3::{
    alloc,
    depot::{capacity, queue::ring},
};

use crate::reply::arena::state;

mod raw;

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

pub(in crate::reply) struct State {
    queued: ring::Cell<Retired>,
}

impl State {
    pub(in crate::reply::arena) fn try_new(
        capacity: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            queued: ring::Cell::try_new(capacity)?,
        })
    }

    pub(in crate::reply::arena) fn pop(&self) -> Option<Retired> {
        self.queued.pop_front()
    }

    pub(in crate::reply::arena) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }
}
