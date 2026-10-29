//! Reusable exact slots whose linear leases may outlive one slot group.

use std::{cell, marker, mem, num, ptr};

use o3::{alloc, alloc::tail, depot::arena::linked::chain};
pub use owner::Owner;
pub use slots::Slots;

use crate::driver::{
    receive::credit::{error, storage},
    route::indices,
};

mod owner;
mod raw;
mod slots;

#[derive(Clone, Copy, PartialEq, Eq)]

enum Availability {
    Free,
    Owned,
    Exhausted,
}

#[derive(Clone, Copy)]

enum Attachment {
    Active,
    Retired,
}

pub(in crate::driver::receive::credit::storage) struct Group<C> {
    owners: cell::Cell<num::NonZero<usize>>,
    generation: cell::Cell<num::NonZero<u64>>,
    context: cell::Cell<ptr::NonNull<C>>,
    availability: cell::Cell<Availability>,
    attachment: cell::Cell<Attachment>,
}

impl<C> Group<C> {
    fn new(context: &C) -> Self {
        Self {
            owners: cell::Cell::new(num::NonZero::<usize>::MIN),
            generation: cell::Cell::new(num::NonZero::<u64>::MIN),
            context: cell::Cell::new(ptr::NonNull::from(context)),
            availability: cell::Cell::new(Availability::Owned),
            attachment: cell::Cell::new(Attachment::Retired),
        }
    }

    fn generation(&self) -> error::Generation {
        let epoch = self.generation.get();
        match self.attachment.get() {
            Attachment::Active => error::Generation::Active(epoch),
            Attachment::Retired => error::Generation::Retired(epoch),
        }
    }

    fn release(&self) {
        self.retire(self.owners.get().get() - 1);
    }

    /// Retains exactly the surviving leases; zero closes this generation.
    fn retire(&self, left: usize) {
        if let Some(rest) = num::NonZero::new(left) {
            self.owners.set(rest);
        } else if let Some(next) = self.generation.get().checked_add(1) {
            self.generation.set(next);
            self.availability.set(Availability::Free);
        } else {
            self.availability.set(Availability::Exhausted);
        }
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Group<usize>>() == 4 * mem::size_of::<usize>());
};

pub(in crate::driver::receive::credit::storage) struct Active<'a, T: Copy, S: Copy, C> {
    pub(in crate::driver::receive::credit::storage) group: &'a Group<C>,
    pub(in crate::driver::receive::credit::storage) entries: &'a [Entry<T, S, C>],
    pub(in crate::driver::receive::credit::storage) capacity: num::NonZero<u32>,
}

impl<T: Copy, S: Copy, C> Drop for Active<'_, T, S, C> {
    fn drop(&mut self) {
        let owners = storage::Slot::iter(self)
            .filter(|slot| slot.is_held())
            .count();
        self.group.attachment.set(Attachment::Retired);
        self.group.retire(owners);
    }
}

pub(in crate::driver::receive::credit::storage) struct Vacant<'a, T: Copy, S: Copy, C> {
    raw: chain::Link<'a, Group<C>, Entry<T, S, C>>,
    entries: &'a [Entry<T, S, C>],
    capacity: num::NonZero<u32>,
}

impl<'a, T: Copy, S: Copy, C> Vacant<'a, T, S, C> {
    pub(in crate::driver::receive::credit::storage) fn select(
        raw: chain::Link<'a, Group<C>, Entry<T, S, C>>,
        capacity: num::NonZero<u32>,
    ) -> Option<Self> {
        let group = raw.header();
        if group.availability.get() != Availability::Free {
            return None;
        }
        let entries = raw.as_slice().get(..capacity.get() as usize)?;
        group.availability.set(Availability::Owned);
        Some(Self {
            raw,
            entries,
            capacity,
        })
    }

    /// Geometric backing bounds history retained by old keys through owner drop.
    pub(in crate::driver::receive::credit::storage) fn allocate(
        groups: &'a chain::List<Group<C>, Entry<T, S, C>>,
        capacity: num::NonZero<u32>,
        context: &'a C,
    ) -> Result<Self, alloc::Error> {
        let count = capacity.get() as usize;
        let extent = count.checked_next_power_of_two().ok_or(alloc::Overflow)?;
        let memory = tail::Plan::try_new(extent)?.try_allocate()?;
        let block = memory.fill(chain::Node::new(Group::new(context)), |node, _| {
            Entry::new(node.header())
        });
        let raw = groups.push(block);
        Ok(Self {
            raw,
            // SAFETY: checked growth bounds count by the fully initialized extent;
            // push retains that exact allocation under this immutable list borrow.
            entries: unsafe { raw.as_slice().get_unchecked(..count) },
            capacity,
        })
    }

    pub(in crate::driver::receive::credit::storage) fn is_smaller(&self, other: &Self) -> bool {
        self.raw.len() < other.raw.len()
    }

    pub(in crate::driver::receive::credit::storage) fn is_exact(&self) -> bool {
        self.raw.len() == self.capacity.get() as usize
    }

    pub(in crate::driver::receive::credit::storage) fn activate(
        self,
        context: &'a C,
    ) -> Active<'a, T, S, C> {
        let this = mem::ManuallyDrop::new(self);
        let group = this.raw.header();
        group.context.set(ptr::NonNull::from(context));
        for entry in this.entries {
            entry.state.set(State::Attached(Occupancy::Idle));
        }
        group.attachment.set(Attachment::Active);
        Active {
            group,
            entries: this.entries,
            capacity: this.capacity,
        }
    }
}

impl<T: Copy, S: Copy, C> Drop for Vacant<'_, T, S, C> {
    fn drop(&mut self) {
        self.raw.header().availability.set(Availability::Free);
    }
}

#[derive(Clone, Copy)]
enum State<S: Copy> {
    Attached(Occupancy<S>),
    Detached,
    DetachedHeld,
}

#[derive(Clone, Copy)]
enum Occupancy<S: Copy> {
    Idle,
    Held,
    Ready(S),
}

pub(in crate::driver::receive::credit::storage) struct Entry<T: Copy, S: Copy, C> {
    header: ptr::NonNull<Group<C>>,
    pub(in crate::driver::receive::credit::storage) state: cell::Cell<State<S>>,
    invariant: marker::PhantomData<cell::Cell<T>>,
}

/// Access whose borrow prevents its exact active slot group from retiring.
pub struct Slot<'a, 'owner, T: Copy, S: Copy, C> {
    entry: &'owner Entry<T, S, C>,
    active: marker::PhantomData<&'a Active<'owner, T, S, C>>,
}

/// A stable slot identity that validates its generation before mutation.
pub struct Key<'owner, T: Copy, S: Copy, C> {
    entry: &'owner Entry<T, S, C>,
    generation: num::NonZero<u64>,
}

/// Owns one armed target and its exact slot contribution until consumed.
#[must_use = "dropping a lease cancels the armed slot"]
pub struct Lease<'owner, T: Copy, S: Copy, C> {
    entry: &'owner Entry<T, S, C>,
    target: T,
}

impl<T: Copy, S: Copy, C> Entry<T, S, C> {
    pub(in crate::driver::receive::credit::storage) fn new(header: &Group<C>) -> Self {
        Self {
            header: ptr::NonNull::from(header),
            state: cell::Cell::new(State::Detached),
            invariant: marker::PhantomData,
        }
    }
}

impl<'a, 'owner, T: Copy, S: Copy, C> Slot<'a, 'owner, T, S, C> {
    pub(in crate::driver::receive::credit::storage) fn new(
        active: &'a Active<'owner, T, S, C>,
        index: indices::SlotIndex,
    ) -> Result<Self, error::Index> {
        Ok(Self {
            entry: active
                .entries
                .get(index.raw() as usize)
                .ok_or(error::Index {
                    requested: index,
                    capacity: active.capacity,
                })?,
            active: marker::PhantomData,
        })
    }

    pub(in crate::driver::receive::credit::storage) fn iter(
        active: &'a Active<'owner, T, S, C>,
    ) -> impl Iterator<Item = Self> + 'a {
        active.entries.iter().map(|entry| Self {
            entry,
            active: marker::PhantomData,
        })
    }

    pub fn key(&self) -> Key<'owner, T, S, C> {
        Key {
            entry: self.entry,
            generation: self.entry.header().generation.get(),
        }
    }

    pub fn is_active(self) -> bool {
        matches!(
            self.entry.state.get(),
            State::Attached(Occupancy::Held | Occupancy::Ready(..)) | State::DetachedHeld
        )
    }

    /// Whether a live lease still owns the slot, including after detachment.
    pub fn is_held(self) -> bool {
        matches!(
            self.entry.state.get(),
            State::Attached(Occupancy::Held) | State::DetachedHeld
        )
    }

    pub fn take(self) -> Option<S> {
        let State::Attached(Occupancy::Ready(signal)) = self.entry.state.get() else {
            return None;
        };
        self.entry.state.set(State::Attached(Occupancy::Idle));
        Some(signal)
    }

    pub fn detach(self) {
        match self.entry.state.get() {
            State::Attached(Occupancy::Idle | Occupancy::Ready(..)) => {
                self.entry.state.set(State::Detached);
            }
            State::Attached(Occupancy::Held) => self.entry.state.set(State::DetachedHeld),
            State::Detached | State::DetachedHeld => {}
        }
    }
}

impl<'owner, T: Copy, S: Copy, C> Key<'owner, T, S, C> {
    pub fn context(self) -> &'owner C {
        self.entry.header().context()
    }

    pub fn arm(self, target: T) -> Result<Lease<'owner, T, S, C>, error::Arm> {
        let group = self.entry.header();
        let observed = group.generation();
        if observed != error::Generation::Active(self.generation) {
            return Err(error::Arm::Stale(error::Stale {
                expected: self.generation,
                observed,
            }));
        }
        match self.entry.state.get() {
            State::Attached(Occupancy::Idle) => {}
            State::Attached(Occupancy::Held | Occupancy::Ready(..)) => {
                return Err(error::Arm::Busy);
            }
            State::Detached | State::DetachedHeld => return Err(error::Arm::Detached),
        }
        self.entry.state.set(State::Attached(Occupancy::Held));
        Ok(Lease {
            entry: self.entry,
            target,
        })
    }
}

impl<T: Copy, S: Copy, C> Lease<'_, T, S, C> {
    pub fn release_with(self, signal: S, notify: impl FnOnce(&C, T)) {
        let this = mem::ManuallyDrop::new(self);
        let entry = this.entry;
        let group = entry.header();
        match group.attachment.get() {
            Attachment::Active => match entry.state.get() {
                State::Attached(_) => {
                    entry.state.set(State::Attached(Occupancy::Ready(signal)));
                    notify(group.context(), this.target);
                }
                State::Detached | State::DetachedHeld => entry.state.set(State::Detached),
            },
            Attachment::Retired => {
                entry.state.set(State::Detached);
                group.release();
            }
        }
    }
}

impl<T: Copy, S: Copy, C> Drop for Lease<'_, T, S, C> {
    fn drop(&mut self) {
        let entry = self.entry;
        let group = entry.header();
        match group.attachment.get() {
            Attachment::Active => match entry.state.get() {
                State::Attached(_) => entry.state.set(State::Attached(Occupancy::Idle)),
                State::Detached | State::DetachedHeld => entry.state.set(State::Detached),
            },
            Attachment::Retired => {
                entry.state.set(State::Detached);
                group.release();
            }
        }
    }
}

impl<T: Copy, S: Copy, C> Clone for Slot<'_, '_, T, S, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy, S: Copy, C> Copy for Slot<'_, '_, T, S, C> {}

impl<T: Copy, S: Copy, C> Clone for Key<'_, T, S, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy, S: Copy, C> Copy for Key<'_, T, S, C> {}

impl<T: Copy, S: Copy, C> PartialEq for Key<'_, T, S, C> {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.entry, other.entry) && self.generation == other.generation
    }
}

impl<T: Copy, S: Copy, C> Eq for Key<'_, T, S, C> {}

const _: () = {
    assert!(
        mem::size_of::<Entry<num::NonZero<u64>, num::NonZero<u8>, usize>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Slot<'_, '_, num::NonZero<u64>, num::NonZero<u8>, usize>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Key<'_, num::NonZero<u64>, num::NonZero<u8>, usize>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Lease<'_, num::NonZero<u64>, num::NonZero<u8>, usize>>()
            == 2 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Lease<'_, num::NonZero<u64>, num::NonZero<u8>, usize>>>()
            == 2 * mem::size_of::<usize>()
    );
};

impl<C> Group<C> {
    pub(super) fn context(&self) -> &C {
        // SAFETY: activation binds the context to the same Owner borrow that
        // every exposed key, lease, and group capability retains.
        unsafe { self.context.get().as_ref() }
    }
}

impl<T: Copy, S: Copy, C> Entry<T, S, C> {
    pub(super) fn header(&self) -> &Group<C> {
        // SAFETY: the owning chain keeps this entry and its header at stable
        // addresses for every key, slot borrow, and lease derived from it.
        unsafe { self.header.as_ref() }
    }
}
