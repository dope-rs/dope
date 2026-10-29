//! Fixed-capacity persistent resident subscriptions.
use std::{cell, marker, mem, ptr};

pub use lease::Lease;
use o3::{
    alloc,
    base::marker::ThreadBound,
    depot,
    depot::{
        capacity,
        queue::{links, resident},
    },
};

use crate::driver::schedule::ready::wake::fanout::storage;

mod lease;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready::wake) struct Key(links::Index);

/// A subscription whose current registry binding rejected an update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected registration must be handled"]
pub enum Reject {
    Active,
    Full,
}

/// One examined subscriber in the current fan-out epoch.
pub enum Step<W> {
    Idle,
    Item(W),
}

/// Fixed-capacity persistent broadcast state.
/// Registration and cancellation are O(1); `step` examines one resident entry.
pub struct Registry<W> {
    entries: Box<[resident::raw::Node<Entry<W>>]>,
    thread: marker::PhantomData<ThreadBound>,
    links: links::Chain,
    free: links::Free,
    cursor: cell::Cell<Option<links::Index>>,
    epoch: cell::Cell<u64>,
    draining: cell::Cell<u64>,
}

impl<W> Registry<W> {
    pub fn try_new(capacity: usize) -> Result<Self, resident::Error> {
        let capacity = capacity::Count::try_from(capacity).map_err(resident::Error::Capacity)?;
        let entries = alloc::BoxSliceExt::try_from_fn(capacity.get(), |index| {
            let index = index as u32;
            let next = index + 1;
            let free = if next < capacity.raw() {
                next
            } else {
                u32::MAX
            };
            let entry = Entry {
                value: resident::raw::Entry::default(),
                delivered: cell::Cell::new(0),
            };
            resident::raw::Node::vacant(entry, index, free)
        })?;
        Ok(Self {
            entries,
            thread: marker::PhantomData,
            links: links::Chain::new(),
            free: links::Free::seeded(capacity.get()),
            cursor: cell::Cell::new(None),
            epoch: cell::Cell::new(0),
            draining: cell::Cell::new(0),
        })
    }

    /// Installs the exact current value and marks the lease current for this epoch.
    pub fn try_keep<'target>(
        &'target self,
        lease: &mut Lease<'target, W>,
        value: W,
    ) -> Result<(), storage::Reject>
    where
        W: Copy,
    {
        match lease.active() {
            Some((active, key)) => {
                if !ptr::eq(active, self) {
                    return Err(storage::Reject::Active);
                }
                active.view(key).initialize(active.epoch.get(), value);
            }
            None => {
                let key = self.try_insert(value)?;
                lease.set(self, key);
            }
        }
        Ok(())
    }

    /// Publishes a new epoch without restarting a partial drain. Publications
    /// during a drain coalesce into its next pass; later registrations skip them.
    pub fn publish(&self) {
        let epoch = self.epoch.get().wrapping_add(2);
        let epoch = if epoch == 0 {
            for entry in self.entries.iter() {
                entry.reset();
            }
            self.draining.set(0);
            2
        } else {
            epoch
        };
        self.epoch.set(epoch);
        if self.cursor.get().is_none() {
            self.draining.set(epoch);
            self.cursor.set(self.links.first());
        }
    }

    /// Examines one current subscriber and returns its installed value.
    /// The cursor and delivery epoch are committed before ownership is returned.
    pub fn step(&self) -> Option<Step<W>> {
        if self.cursor.get().is_none() && self.draining.get() != self.epoch.get() {
            self.draining.set(self.epoch.get());
            self.cursor.set(self.links.first());
        }
        let index = self.cursor.get()?;
        let view = self.view(Key(index));
        self.cursor.set(view.hook().next_index());
        Some(view.step(self.draining.get()))
    }

    pub fn has_pending(&self) -> bool {
        self.cursor.get().is_some() || self.draining.get() != self.epoch.get()
    }
    fn try_insert(&self, value: W) -> Result<Key, storage::Reject>
    where
        W: Copy,
    {
        let Some(view) =
            (unsafe { resident::raw::View::acquire(&self.free, self.entries.as_ref()) })
        else {
            return Err(storage::Reject::Full);
        };
        let index = *view.key();
        unsafe {
            depot::raw::Hooks::chain_push(
                resident::raw::Slice::new(self.entries.as_ref()),
                &self.links,
                index,
            )
        };
        view.initialize(self.epoch.get(), value);
        Ok(Key(index))
    }

    fn view(&self, key: Key) -> resident::raw::View<'_, Entry<W>> {
        unsafe { resident::raw::View::new(self.entries.as_ref(), key.0) }
    }
}

impl<W> Registry<W> {
    fn retire(&self, key: Key) {
        let view = self.view(key);
        if self.cursor.get() == Some(*view.key()) {
            self.cursor.set(view.hook().next_index());
        }
        let vacant = unsafe {
            depot::raw::Hooks::chain_unlink(
                resident::raw::Slice::new(self.entries.as_ref()),
                &self.links,
                *view.key(),
            )
        };
        self.free.recycle(vacant);
        view.retire();
    }
}

const _: () = assert!(mem::size_of::<Registry<u64>>() == 6 * mem::size_of::<usize>());

const PRESENT: u64 = 1;

struct Entry<W> {
    value: resident::raw::Entry<W>,
    delivered: cell::Cell<u64>,
}

impl<W> Entry<W> {
    fn initialize(&self, epoch: u64, value: W)
    where
        W: Copy,
    {
        self.value.initialize(value);
        self.delivered.set(epoch | PRESENT);
    }

    fn step(&self, epoch: u64) -> Step<W> {
        let state = self.delivered.get();
        if state & !PRESENT >= epoch {
            return Step::Idle;
        }
        self.delivered.set(epoch);
        if state & PRESENT == 0 {
            return Step::Idle;
        }
        // SAFETY: PRESENT is the payload initialization tag.
        Step::Item(unsafe { self.value.take() })
    }
    fn retire(&self) {
        let state = self.delivered.get();
        self.delivered.set(state & !PRESENT);
        if state & PRESENT != 0 {
            // SAFETY: PRESENT retains this slot's initialized payload.
            unsafe { self.value.retire() };
        }
    }

    fn reset(&self) {
        self.delivered.set(self.delivered.get() & PRESENT);
    }
}
