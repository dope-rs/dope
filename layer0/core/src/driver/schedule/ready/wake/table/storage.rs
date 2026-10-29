//! Indexed resident entries and their active FIFO.
use std::{marker, mem, ptr};

use o3::{
    alloc,
    base::marker::ThreadBound,
    depot,
    depot::{
        capacity,
        queue::{links, resident},
    },
    mem::quota,
};

use crate::driver::schedule::{app, ready::wake::table::storage};

/// One affine value slot per exact identity with an active-only FIFO.
pub struct Registry<W> {
    entries: Entries<W>,
    draining: resident::Draining,
}

/// One compact index checked against the immutably borrowed table's extent.
#[derive(Clone, Copy)]
pub struct Slot<'a, W> {
    pub(super) table: &'a Registry<W>,
    pub(super) key: Key,
}

/// A registration whose exact table binding rejected an update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected registration must be handled"]
pub enum Reject {
    Active { index: usize },
    Draining { index: usize },
    Occupied { index: usize },
}

impl<W> Registry<W> {
    pub fn try_new(capacity: usize) -> Result<Self, resident::Error> {
        Ok(Self {
            entries: Entries::try_new(capacity)?,
            draining: resident::Draining::default(),
        })
    }

    pub fn try_slot(&self, index: usize) -> Result<Slot<'_, W>, resident::Bounds<usize>> {
        let key = self.entries.try_key(index)?;
        Ok(Slot { table: self, key })
    }

    pub fn has_waiters(&self) -> bool {
        self.entries.has_waiters()
    }

    pub fn begin_drain(&self) {
        self.draining.begin();
    }

    pub fn is_draining(&self) -> bool {
        self.draining.is_active()
    }

    /// Takes the next value in drain order and ends draining at quiescence.
    pub(super) fn drain_next(&self, work: app::Work<'_, '_>) -> quota::Admission<W> {
        match self.draining.drain(|| {
            let slot = Slot {
                table: self,
                key: Key(self.entries.links.first()?),
            };
            let Some(permit) = work.permit() else {
                return Some(quota::Admission::Exhausted);
            };
            Some(quota::Admission::Item(permit.run(|| {
                let view = slot.view();
                // SAFETY: admission cannot reenter this table, so the selected
                // front still belongs to its exact reciprocal chain and store.
                let (_, vacant) = unsafe {
                    depot::raw::Hooks::chain_pop(
                        resident::raw::Slice::new(&self.entries.entries),
                        &self.entries.links,
                        slot.key.0,
                    )
                };
                vacant.park();
                // SAFETY: the detached front owned one initialized value, and
                // parking preserved that value in its original table slot.
                unsafe { view.take() }
            })))
        }) {
            resident::Drain::Done => quota::Admission::Empty,
            resident::Drain::Item(value) => value,
        }
    }
}

impl<'a, W> Slot<'a, W> {
    pub fn try_keep(self, token: &mut Registration<'a, W>, value: W) -> Result<(), Reject>
    where
        W: Copy,
    {
        if self.table.is_draining() {
            return Err(Reject::Draining {
                index: self.key.index(),
            });
        }
        token.try_keep(self, value)
    }

    pub fn take_one(self) -> resident::Take<W> {
        let view = self.view();
        if !self.key.0.is_linked(view.hook()) {
            return resident::Take::Empty;
        }
        self.unlink().park();
        // SAFETY: the linked slot owns one initialized value; unlink removed its only delivery.
        resident::Take::Item(unsafe { view.take() })
    }
}

const _: () = {
    assert!(mem::size_of::<Slot<'static, usize>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Registration<'static, usize>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Registration<'static, usize>>() == mem::align_of::<usize>());
};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(super) struct Key(pub(super) links::Index);

struct Entries<W> {
    entries: Box<[resident::raw::Node<resident::raw::Entry<W>>]>,
    thread: marker::PhantomData<ThreadBound>,
    links: links::Chain,
}

/// A linear registration in a [`storage::Registry`].
#[repr(transparent)]
pub struct Registration<'target, W> {
    active: Option<Occupied<'target, W>>,
}

/// A retained registration owning a linked value or a parked, empty slot.
struct Occupied<'a, W> {
    slot: Slot<'a, W>,
}

impl<'a, W> Default for Registration<'a, W> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, W> Registration<'a, W> {
    pub const fn new() -> Self {
        Self { active: None }
    }

    fn try_keep(&mut self, slot: Slot<'a, W>, value: W) -> Result<(), storage::Reject>
    where
        W: Copy,
    {
        match &mut self.active {
            Some(active) => active.try_keep(slot, value)?,
            None => self.active = Some(slot.try_insert(value)?),
        }
        Ok(())
    }
}

impl Key {
    const fn index(self) -> usize {
        self.0.get()
    }
}

impl<W> Entries<W> {
    fn try_new(capacity: usize) -> Result<Self, resident::Error> {
        let capacity = capacity::Count::try_from(capacity).map_err(resident::Error::Capacity)?;
        let entries = alloc::BoxSliceExt::try_from_fn(capacity.get(), |index| {
            resident::raw::Node::vacant(resident::raw::Entry::default(), index as u32, u32::MAX)
        })?;
        Ok(Self {
            entries,
            thread: marker::PhantomData,
            links: links::Chain::new(),
        })
    }

    fn try_key(&self, index: usize) -> Result<Key, resident::Bounds<usize>> {
        let error = resident::Bounds::new(index, self.entries.len());
        let key = links::Index::try_new(index)
            .filter(|key| key.get() < self.entries.len())
            .ok_or(error)?;
        Ok(Key(key))
    }

    fn has_waiters(&self) -> bool {
        !self.links.is_empty()
    }
}

impl<'a, W> Slot<'a, W> {
    fn view(&self) -> resident::raw::View<'a, resident::raw::Entry<W>> {
        // SAFETY: try_slot checked this key, or drain_next selected its linked
        // front from the same immutable extent. Slot retains that exact table.
        unsafe { resident::raw::View::new(&self.table.entries.entries, self.key.0) }
    }

    fn try_insert(self, value: W) -> Result<Occupied<'a, W>, storage::Reject>
    where
        W: Copy,
    {
        let view = self.view();
        if !view.hook().is_vacant(self.key.index()) {
            return Err(storage::Reject::Occupied {
                index: self.key.index(),
            });
        }
        view.initialize(value);
        let entries = &self.table.entries;
        // SAFETY: the checked slot is vacant and initialized above. Only this
        // table mutates its stable hooks, preserving the chain's reciprocal ends.
        unsafe {
            depot::raw::Hooks::chain_push(
                resident::raw::Slice::new(&entries.entries),
                &entries.links,
                self.key.0,
            )
        };
        Ok(Occupied { slot: self })
    }

    fn unlink(&self) -> links::Detached<'a> {
        let entries = &self.table.entries;
        // SAFETY: take_one proves this slot linked; Occupied's exclusive
        // registration otherwise owns either this linked slot or its parked
        // marker. Its drop calls here only after excluding that parked state.
        unsafe {
            depot::raw::Hooks::chain_unlink(
                resident::raw::Slice::new(&entries.entries),
                &entries.links,
                self.key.0,
            )
        }
    }
}

impl<W> Occupied<'_, W> {
    fn try_keep(&mut self, slot: Slot<'_, W>, value: W) -> Result<(), storage::Reject>
    where
        W: Copy,
    {
        if !ptr::eq(self.slot.table, slot.table) || self.slot.key != slot.key {
            return Err(storage::Reject::Active {
                index: slot.key.index(),
            });
        }
        let view = self.slot.view();
        view.initialize(value);
        if let Some(vacant) = view.unpark() {
            // SAFETY: this registration retains the exact table owning both
            // the detached hook and the active chain's reciprocal endpoints.
            unsafe {
                depot::raw::Hooks::push(
                    resident::raw::Slice::new(&self.slot.table.entries.entries),
                    vacant,
                    &self.slot.table.entries.links,
                )
            };
        }
        Ok(())
    }
}

impl<W> Drop for Occupied<'_, W> {
    fn drop(&mut self) {
        let view = self.slot.view();
        if let Some(vacant) = view.unpark() {
            vacant.idle();
        } else {
            self.slot.unlink().idle();
            // SAFETY: this registration removed its linked initialized payload exactly once.
            unsafe { view.retire() };
        }
    }
}

const _: () = assert!(mem::size_of::<Entries<u64>>() == 3 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Option<Occupied<'static, u64>>>() == 2 * mem::size_of::<usize>());
