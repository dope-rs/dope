use dope_sys::{
    io::reactor::read,
    table::{change::Id as _, registry, registry::Entry as _},
};
use o3::depot::{
    self,
    map::{self, flat},
    queue::indexed::fifo,
};

use crate::{
    reactor::input::{Slot, entry::Id},
    store::hash,
};

pub struct Occupied<'a, C: read::Context> {
    pub(super) slot: map::Found<&'a mut flat::Table<Slot<C>>>,
    pub(super) fd: map::Found<&'a mut flat::Table<(usize, Id)>>,
    pub(super) resume: &'a mut fifo::List<Id>,
}

impl<'a, C: read::Context> Occupied<'a, C> {
    pub(super) fn new(
        slot: map::Found<&'a mut flat::Table<Slot<C>>>,
        by_fd: &'a mut flat::Table<(usize, Id)>,
        resume: &'a mut fifo::List<Id>,
    ) -> Self {
        let raw = slot.get().file().fd() as usize;
        // SAFETY: the retained primary entry owns this immutable fd identity;
        // both indices are inserted and removed together under exclusive loans.
        let source = unsafe {
            depot::raw::Proof::new((by_fd, slot, hash(raw), |entry: &(usize, Id)| entry.0 == raw))
        };
        let (fd, slot) = flat::Table::member(source);
        Self { slot, fd, resume }
    }
}

impl<C: read::Context> registry::Occupied for Occupied<'_, C> {
    type Entry = Slot<C>;

    fn get(&self) -> &Slot<C> {
        self.slot.get()
    }

    fn remove(self) -> Slot<C> {
        self.fd.remove();
        let slot = self.slot.remove();
        if let Some(fifo::Entry::Occupied(entry)) = self.resume.entry(slot.index())
            && slot.id() == *entry.get()
        {
            entry.clear();
        }
        slot
    }
}
