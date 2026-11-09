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
    entries: Box<[resident::raw::Node<resident::raw::Entry<W>>]>,
    thread: marker::PhantomData<ThreadBound>,
    links: links::Chain,
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
        let capacity = capacity::Count::try_from(capacity).map_err(resident::Error::Capacity)?;
        let entries = alloc::BoxSliceExt::try_from_fn(capacity.get(), |index| {
            resident::raw::Node::vacant(resident::raw::Entry::default(), index as u32, u32::MAX)
        })?;
        Ok(Self {
            entries,
            thread: marker::PhantomData,
            links: links::Chain::new(),
            draining: resident::Draining::default(),
        })
    }

    pub fn try_slot(&self, index: usize) -> Result<Slot<'_, W>, resident::Bounds<usize>> {
        let key = self.try_key(index)?;
        Ok(Slot { table: self, key })
    }

    pub fn has_waiters(&self) -> bool {
        !self.links.is_empty()
    }

    pub fn begin_drain(&self) {
        self.draining.begin();
    }

    pub fn is_draining(&self) -> bool {
        self.draining.is_active()
    }

    /// Takes the next value in drain order and ends draining at quiescence.
    pub(super) fn drain_next(&self, work: app::Work<'_, '_>) -> quota::Admission<W> {
        self.draining
            .drain(|| {
                let slot = Slot {
                    table: self,
                    key: Key(self.links.first()?),
                };
                let Some(permit) = work.permit() else {
                    work.defer();
                    return Some(quota::Admission::Exhausted);
                };
                Some(quota::Admission::Item(permit.run(|| {
                    let view = slot.view();
                    // SAFETY: admission cannot reenter this table, so the selected
                    // front still belongs to its exact reciprocal chain and store.
                    let vacant = unsafe {
                        depot::raw::Hooks::chain_pop(self.entries.as_ref(), &self.links, slot.key.0)
                    };
                    vacant.park();
                    // SAFETY: the detached front owned one initialized value, and
                    // parking preserved that value in its original table slot.
                    unsafe { view.take() }
                })))
            })
            .unwrap_or(quota::Admission::Empty)
    }

    fn try_key(&self, index: usize) -> Result<Key, resident::Bounds<usize>> {
        let error = resident::Bounds::new(index, self.entries.len());
        let key = links::Index::try_new(index)
            .filter(|key| key.get() < self.entries.len())
            .ok_or(error)?;
        Ok(Key(key))
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

    pub fn take_one(self) -> Option<W> {
        let view = self.view();
        if !self.key.0.is_linked(view.hook()) {
            return None;
        }
        self.unlink().park();
        // SAFETY: the linked slot owns one initialized value; unlink removed its only delivery.
        Some(unsafe { view.take() })
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

/// A linear registration in a [`storage::Registry`].
#[repr(transparent)]
pub struct Registration<'target, W> {
    active: Option<Slot<'target, W>>,
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
            Some(active) => {
                if !ptr::eq(active.table, slot.table) || active.key != slot.key {
                    return Err(storage::Reject::Active {
                        index: slot.key.index(),
                    });
                }
                let view = active.view();
                view.initialize(value);
                if let Some(vacant) = view.unpark() {
                    // SAFETY: this registration retains the exact table owning both
                    // the detached hook and the active chain's reciprocal endpoints.
                    unsafe {
                        depot::raw::Hooks::push(
                            active.table.entries.as_ref(),
                            vacant,
                            &active.table.links,
                        )
                    };
                }
            }
            None => {
                let view = slot.view();
                if !view.hook().is_vacant(slot.key.index()) {
                    return Err(storage::Reject::Occupied {
                        index: slot.key.index(),
                    });
                }
                view.initialize(value);
                let table = slot.table;
                // SAFETY: the checked slot is vacant and initialized above. Only this
                // table mutates its stable hooks, preserving the chain's reciprocal ends.
                unsafe {
                    depot::raw::Hooks::chain_push(table.entries.as_ref(), &table.links, slot.key.0)
                };
                self.active = Some(slot);
            }
        }
        Ok(())
    }
}

impl Key {
    const fn index(self) -> usize {
        self.0.get()
    }
}

impl<'a, W> Slot<'a, W> {
    fn view(&self) -> resident::raw::View<'a, resident::raw::Entry<W>> {
        // SAFETY: try_slot checked this key, or drain_next selected its linked
        // front from the same immutable extent. Slot retains that exact table.
        unsafe { resident::raw::View::new(&self.table.entries, self.key.0) }
    }

    fn unlink(&self) -> links::Detached<'a> {
        let table = self.table;
        // SAFETY: take_one proves this slot linked; the exclusive
        // registration otherwise owns either this linked slot or its parked
        // marker. Its drop calls here only after excluding that parked state.
        unsafe { depot::raw::Hooks::chain_unlink(table.entries.as_ref(), &table.links, self.key.0) }
    }
}

impl<W> Drop for Registration<'_, W> {
    fn drop(&mut self) {
        let Some(slot) = &self.active else {
            return;
        };
        let view = slot.view();
        if let Some(vacant) = view.unpark() {
            vacant.idle();
        } else {
            slot.unlink().idle();
            // SAFETY: this registration removed its linked initialized payload exactly once.
            unsafe { view.retire() };
        }
    }
}

const _: () = assert!(mem::size_of::<Registry<u64>>() == 4 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Option<Slot<'static, u64>>>() == 2 * mem::size_of::<usize>());
