use std::mem;

use o3::base::num;

use crate::{
    driver::{
        route,
        route::{indices, indices::SlotIndex},
    },
    io::fd::{domain::resident, handles, identity},
};

const SIDE_BIT: u32 = 1 << route::SLOT_BITS;

pub(in crate::io::fd::domain) struct Retirement {
    index_and_side: u32,
    generation: num::Generation,
}

#[repr(u8)]
pub(in crate::io::fd::domain) enum Reuse {
    Reusable,
    Exhausted,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::io::fd::domain) struct View<'a>(&'a resident::Store);

#[must_use = "a validated descriptor release must settle or enqueue its registry entry"]
pub(in crate::io::fd::domain) struct Release<'a> {
    store: &'a resident::Store,
    index: indices::SlotIndex,
    entry: resident::Entry<'a>,
    phase: resident::Phase,
}

impl<'a> View<'a> {
    pub(in crate::io::fd::domain) const fn new(store: &'a resident::Store) -> Self {
        Self(store)
    }

    /// An exhausted slot stays occupied in the native file table until reactor
    /// teardown; its quiescent, unleased entry cannot admit another owner or close.
    pub(in crate::io::fd::domain) fn abandon(
        self,
        accepted: handles::Accepted,
    ) -> handles::Reclaimed {
        let slot = handles::PhysicalSlot::from(accepted);
        let index = SlotIndex::from(slot);
        let entry = self.0.bind(resident::Bound::slot(slot));
        if entry.is_exhausted() {
            entry.set_status(resident::Phase::QUARANTINED, false);
            return handles::Reclaimed::Quarantined(slot);
        }
        entry.advance();
        entry.set_status(resident::Phase::Pending, false);
        queue(self.0, index, &entry);
        handles::Reclaimed::Queued(slot)
    }

    pub(in crate::io::fd::domain) fn release(self, id: identity::Identity<'_>) -> Release<'a> {
        let slot = id.slot();
        let index = SlotIndex::from(slot);
        let entry = self.0.bind(resident::Bound::slot(slot));
        let phase = entry.phase();
        Release {
            store: self.0,
            index,
            phase,
            entry,
        }
    }

    pub(in crate::io::fd::domain) fn enqueue(self, slot: handles::PhysicalSlot) {
        let index = SlotIndex::from(slot);
        let entry = self.0.bind(resident::Bound::slot(slot));
        entry.set_phase(resident::Phase::Pending);
        queue(self.0, index, &entry);
    }

    pub(in crate::io::fd::domain) fn pending(self) -> Option<Retirement> {
        let (index, side) = self.0.pending.front()?;
        let entry = self.0.entry(index)?;
        if entry.phase() != resident::Phase::Pending {
            return None;
        }
        let epoch = if entry.has_lease() {
            entry.generation.get()
        } else {
            entry.retired()?
        };
        Some(Retirement::new(index, epoch, side))
    }

    pub(in crate::io::fd::domain) fn has_inflight(self) -> bool {
        !self.0.inflight.get().is_empty()
    }

    pub(in crate::io::fd::domain) fn bind(self, token: Retirement) -> Option<Retirement> {
        let entry = self.0.entry(token.index())?;
        let meta = entry.words.meta().get();
        let expected = resident::INFLIGHT_MASK | side(token.side());
        if meta & resident::RETIREMENT_MASK != expected
            || entry.generation.submitted() != token.generation
        {
            return None;
        }
        entry.words.meta().set(meta | resident::BOUND_MASK);
        Some(token)
    }

    pub(in crate::io::fd::domain) fn submitted(self, token: Retirement) {
        let entry = self.0.bind(resident::Bound::retirement(&token));
        let link = (entry.words.identity().get() & resident::LINK_MASK) as u32;
        self.0.pending.commit(token.side(), link);
        self.0.inflight.set(self.0.inflight.get().submit());
        entry.generation.submit(token.generation);
        let meta = entry.words.meta().get() & !resident::RETIREMENT_MASK;
        entry
            .words
            .meta()
            .set(meta | resident::INFLIGHT_MASK | side(token.side()));
        entry.set_phase(resident::Phase::InFlight);
    }

    pub(in crate::io::fd::domain) fn commit(self, token: Retirement) {
        let entry = self.0.bind(resident::Bound::retirement(&token));
        let link = (entry.words.identity().get() & resident::LINK_MASK) as u32;
        self.0.pending.commit(token.side(), link);
        settle(&entry);
    }

    pub(in crate::io::fd::domain) fn recycle(self, token: Retirement) -> Reuse {
        let entry = self.0.bind(resident::Bound::retirement(&token));
        let link = (entry.words.identity().get() & resident::LINK_MASK) as u32;
        self.0.pending.commit(token.side(), link);
        settle(&entry);
        reuse(&entry)
    }

    pub(in crate::io::fd::domain) fn retry(self, token: Retirement) {
        let entry = self.consume(&token);
        if is_current(&entry, token.generation) {
            entry.set_phase(resident::Phase::Pending);
        }
        self.promote(&token, &entry);
    }

    pub(in crate::io::fd::domain) fn finish(self, token: Retirement) {
        let entry = self.consume(&token);
        if is_current(&entry, token.generation) {
            settle(&entry);
        }
        self.promote(&token, &entry);
    }

    pub(in crate::io::fd::domain) fn finish_reuse(self, token: Retirement) -> Reuse {
        let entry = self.consume(&token);
        settle(&entry);
        reuse(&entry)
    }

    /// Consumes the bound retirement independently of the current descriptor owner.
    fn consume(self, token: &Retirement) -> resident::Entry<'a> {
        let entry = self.0.bind(resident::Bound::retirement(token));
        let meta = entry.words.meta().get() & !resident::RETIREMENT_MASK;
        entry.words.meta().set(meta);
        self.0.inflight.set(self.0.inflight.get().finish());
        entry
    }

    fn promote(self, token: &Retirement, entry: &resident::Entry<'_>) {
        if entry.phase() == resident::Phase::Pending {
            queue(self.0, token.index(), entry);
        }
    }
}

fn side(side: resident::Side) -> u64 {
    match side {
        resident::Side::Zero => 0,
        resident::Side::One => resident::SIDE_MASK,
    }
}

/// Defers replacement retirement until the preceding submitted echo is consumed.
fn queue(store: &resident::Store, index: SlotIndex, entry: &resident::Entry<'_>) {
    if entry.words.meta().get() & resident::INFLIGHT_MASK == 0 {
        store.pending.push(index, entry.words.identity());
    }
}

fn reuse(entry: &resident::Entry<'_>) -> Reuse {
    if entry.is_exhausted() {
        Reuse::Exhausted
    } else {
        Reuse::Reusable
    }
}

fn is_current(entry: &resident::Entry<'_>, epoch: num::Generation) -> bool {
    if !entry.phase().is_inflight() {
        return false;
    }
    if entry.has_lease() {
        entry.generation.get() == epoch
    } else {
        entry.retired() == Some(epoch)
    }
}

fn settle(entry: &resident::Entry<'_>) {
    if entry.has_lease() {
        entry.set_phase(resident::Phase::Quiescent);
    } else {
        entry.recycle();
    }
}

impl Release<'_> {
    pub(in crate::io::fd::domain) fn cancel(self) {
        self.entry.set_status(resident::Phase::Ready, false);
    }

    pub(in crate::io::fd::domain) fn enqueue(self) {
        self.entry.advance();
        self.entry.set_status(resident::Phase::Pending, false);
        queue(self.store, self.index, &self.entry);
    }

    pub(in crate::io::fd::domain) fn release(self) {
        self.entry.advance();
        match self.phase {
            resident::Phase::Ready | resident::Phase::Quiescent => self.entry.recycle(),
            resident::Phase::Pending | resident::Phase::InFlight => {
                self.entry.set_status(self.phase, false);
            }
        }
    }
}

const _: () = {
    assert!(SIDE_BIT as u64 == route::SLOT_MASK + 1);
    assert!(mem::size_of::<resident::Phase>() == mem::size_of::<u8>());
    assert!(mem::size_of::<Retirement>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Reuse>() == mem::size_of::<u8>());
};

impl Retirement {
    pub(in crate::io::fd::domain) const fn new(
        index: indices::SlotIndex,
        epoch: num::Generation,
        side: resident::Side,
    ) -> Self {
        Self {
            index_and_side: index.raw()
                | match side {
                    resident::Side::Zero => 0,
                    resident::Side::One => SIDE_BIT,
                },
            generation: epoch,
        }
    }

    pub(in crate::io::fd::domain) const fn index(&self) -> indices::SlotIndex {
        indices::SlotIndex::from_field(self.index_and_side as u64)
    }

    pub(in crate::io::fd::domain) const fn generation(&self) -> num::Generation {
        self.generation
    }

    pub(in crate::io::fd::domain) const fn side(&self) -> resident::Side {
        if self.index_and_side & SIDE_BIT == 0 {
            resident::Side::Zero
        } else {
            resident::Side::One
        }
    }
}
