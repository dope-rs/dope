use core::{mem, pin::Pin};

use o3::depot;

use crate::task::storage::fixed::schedule::ready::{
    Global,
    linked::{Entry, Side, entry::Value},
};

#[derive(Default)]
pub(in crate::task::storage::fixed::schedule::ready::linked) struct Store {
    sides: [depot::raw::Links<Value>; 2],
}

#[must_use = "a front retains its exact list and pinned entry"]
pub(in crate::task::storage::fixed::schedule::ready::linked) struct Front<'a> {
    list: &'a depot::raw::Links<Value>,
    entry: Pin<&'a depot::raw::Entry<Value>>,
}

impl Store {
    /// Unlinks one attached entry from its side.
    ///
    /// Taking its side consumes membership before another owner can observe it.
    pub(in crate::task::storage::fixed::schedule::ready::linked) fn detach(
        &self,
        entry: Pin<&Entry>,
    ) {
        let entry = entry.link();
        if let Some(side) = entry.get_ref().value().detach() {
            unsafe { self.sides[side.index()].remove(entry) };
        }
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn enqueue(
        &self,
        side: Side,
        entry: Pin<&Entry>,
    ) {
        let entry = entry.link();
        entry.get_ref().value().mark(side);
        unsafe { self.sides[side.index()].push_back(entry) };
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn is_empty(
        &self,
        side: Side,
    ) -> bool {
        self.sides[side.index()].is_empty()
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn peek(
        &self,
        side: Side,
    ) -> Option<Global> {
        Some(self.sides[side.index()].front()?.get_ref().value().index())
    }

    pub(in crate::task::storage::fixed::schedule::ready::linked) fn front(
        &self,
        side: Side,
    ) -> Option<Front<'_>> {
        let list = &self.sides[side.index()];
        let entry = list.front()?;
        Some(Front { list, entry })
    }
}

impl Front<'_> {
    pub(in crate::task::storage::fixed::schedule::ready::linked) fn index(&self) -> Global {
        self.entry.get_ref().value().index()
    }

    /// Clears membership before publishing selection, even if it is forgotten.
    pub(in crate::task::storage::fixed::schedule::ready::linked) fn remove(self) -> Global {
        // SAFETY: this front retains its exact list and pinned entry, unchanged
        // by the quota-only admission callback before removal.
        unsafe { self.list.remove(self.entry) };
        let _ = self.entry.get_ref().value().detach();
        self.index()
    }
}

const _: () = {
    assert!(mem::size_of::<Front<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Front<'static>>>() == mem::size_of::<Front<'static>>());
};
