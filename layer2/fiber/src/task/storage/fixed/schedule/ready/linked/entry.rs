use core::{cell, pin::Pin};

use o3::depot;

use crate::task::storage::fixed::schedule::ready::{Global, linked::Side};

#[repr(C)]
pub(super) struct Value {
    index: Global,
    side: cell::Cell<Option<Side>>,
}

#[repr(transparent)]
pub struct Entry {
    raw: depot::raw::Entry<Value>,
}

impl Entry {
    pub(in crate::task::storage::fixed::schedule) fn index(&self) -> Global {
        self.raw.value().index()
    }

    pub(in crate::task::storage::fixed::schedule) const fn new(index: Global) -> Self {
        Self {
            raw: depot::raw::Entry::new(Value::new(index)),
        }
    }

    pub(super) fn link(self: Pin<&Self>) -> Pin<&depot::raw::Entry<Value>> {
        unsafe { self.map_unchecked(|entry| &entry.raw) }
    }

    pub(super) fn is_queued(&self) -> bool {
        self.raw.value().side.get().is_some()
    }
}

impl Value {
    const fn new(index: Global) -> Self {
        Self {
            index,
            side: cell::Cell::new(None),
        }
    }

    pub(super) fn index(&self) -> Global {
        self.index
    }

    pub(super) fn mark(&self, side: Side) {
        self.side.set(Some(side));
    }

    pub(super) fn detach(&self) -> Option<Side> {
        self.side.replace(None)
    }
}
