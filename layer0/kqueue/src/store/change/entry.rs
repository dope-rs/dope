use std::cell::Cell;

use o3::depot::index::set;

/// One checked event cell with its original membership and count owners.
pub(super) struct Entry<'a> {
    pub(super) value: &'a Cell<Option<libc::kevent>>,
    pub(super) pending: set::Entry<&'a set::State<(), usize, 2>, (), usize, 2>,
    pub(super) count: &'a Cell<usize>,
}

impl Entry<'_> {
    pub(super) fn get(&self) -> Option<libc::kevent> {
        self.value.get()
    }

    pub(super) fn set(&self, event: Option<libc::kevent>) {
        match (self.value.get().is_some(), event.is_some()) {
            (false, true) => {
                self.pending.set(set::Id::at::<0>());
                self.count.set(self.count.get() + 1);
            }
            (true, false) => {
                self.pending.clear(set::Id::at::<0>());
                self.count.set(self.count.get() - 1);
            }
            _ => {}
        }
        self.value.set(event);
    }
}
