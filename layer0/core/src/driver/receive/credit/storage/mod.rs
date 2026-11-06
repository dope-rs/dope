//! Reusable exact slots whose linear leases may outlive one slot group.

use std::{cell, marker, mem, num, ptr};

pub use owner::Owner;
pub use slots::Slots;

use crate::driver::receive::credit::error;

mod owner;
mod raw;
mod slots;

#[derive(Clone, Copy)]

enum Attachment {
    Active,
    Retired,
}

pub(in crate::driver::receive::credit::storage) struct Group<C> {
    owners: cell::Cell<num::NonZero<usize>>,
    generation: cell::Cell<num::NonZero<u64>>,
    context: cell::Cell<ptr::NonNull<C>>,
    available: cell::Cell<bool>,
    attachment: cell::Cell<Attachment>,
}

impl<C> Group<C> {
    fn new(context: &C) -> Self {
        Self {
            owners: cell::Cell::new(num::NonZero::<usize>::MIN),
            generation: cell::Cell::new(num::NonZero::<u64>::MIN),
            context: cell::Cell::new(ptr::NonNull::from(context)),
            available: cell::Cell::new(false),
            attachment: cell::Cell::new(Attachment::Active),
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
            self.available.set(true);
        }
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Group<usize>>() == 4 * mem::size_of::<usize>());
};

#[derive(Clone, Copy)]
enum State<S: Copy> {
    Idle,
    Held,
    Ready(S),
    Detached,
    DetachedHeld,
}

pub(in crate::driver::receive::credit::storage) struct Entry<T: Copy, S: Copy, C> {
    header: ptr::NonNull<Group<C>>,
    pub(in crate::driver::receive::credit::storage) state: cell::Cell<State<S>>,
    invariant: marker::PhantomData<cell::Cell<T>>,
}

/// Access whose borrow prevents its exact active slot group from retiring.
pub struct Slot<'a, 'owner, T: Copy, S: Copy, C> {
    entry: &'owner Entry<T, S, C>,
    active: marker::PhantomData<&'a Slots<'owner, T, S, C>>,
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
            state: cell::Cell::new(State::Idle),
            invariant: marker::PhantomData,
        }
    }

    fn arm(&self, target: T) -> Result<Lease<'_, T, S, C>, error::Arm> {
        match self.state.get() {
            State::Idle => {}
            State::Held | State::Ready(..) => return Err(error::Arm::Busy),
            State::Detached | State::DetachedHeld => return Err(error::Arm::Detached),
        }
        self.state.set(State::Held);
        Ok(Lease {
            entry: self,
            target,
        })
    }
}

impl<'a, 'owner, T: Copy, S: Copy, C> Slot<'a, 'owner, T, S, C> {
    pub fn key(&self) -> Key<'owner, T, S, C> {
        Key {
            entry: self.entry,
            generation: self.entry.header().generation.get(),
        }
    }

    pub fn arm(self, target: T) -> Result<Lease<'owner, T, S, C>, error::Arm> {
        self.entry.arm(target)
    }

    pub fn is_active(self) -> bool {
        matches!(
            self.entry.state.get(),
            State::Held | State::Ready(..) | State::DetachedHeld
        )
    }

    /// Whether a live lease still owns the slot, including after detachment.
    pub fn is_held(self) -> bool {
        matches!(self.entry.state.get(), State::Held | State::DetachedHeld)
    }

    pub fn take(self) -> Option<S> {
        let State::Ready(signal) = self.entry.state.get() else {
            return None;
        };
        self.entry.state.set(State::Idle);
        Some(signal)
    }

    pub fn detach(self) {
        match self.entry.state.get() {
            State::Idle | State::Ready(..) => self.entry.state.set(State::Detached),
            State::Held => self.entry.state.set(State::DetachedHeld),
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
        self.entry.arm(target)
    }
}

impl<T: Copy, S: Copy, C> Lease<'_, T, S, C> {
    pub fn release_with(self, signal: S, notify: impl FnOnce(&C, T)) {
        let this = mem::ManuallyDrop::new(self);
        if let Some(group) = this.finish(State::Ready(signal)) {
            notify(group.context(), this.target);
        }
    }

    fn finish(&self, next: State<S>) -> Option<&Group<C>> {
        let entry = self.entry;
        let group = entry.header();
        if matches!(group.attachment.get(), Attachment::Retired) {
            // Retired slots cannot be observed; reactivation initializes their exact extent.
            group.release();
            return None;
        }
        // This sole lease keeps the slot Held or DetachedHeld until this transition.
        let attached = matches!(entry.state.get(), State::Held);
        entry
            .state
            .set(if attached { next } else { State::Detached });
        attached.then_some(group)
    }
}

impl<T: Copy, S: Copy, C> Drop for Lease<'_, T, S, C> {
    fn drop(&mut self) {
        let _ = self.finish(State::Idle);
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
