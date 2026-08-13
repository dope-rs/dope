#![doc = include_str!("compile_fail.md")]

use std::{io, marker, mem};

use o3::{
    self, cell,
    collections::{self, batch::set},
};

use crate::{
    driver::{
        self,
        route::{self, kind},
        schedule::{self, credits},
        settings,
    },
    io::fd::handles,
};

pub(in crate::driver) mod credit;
mod fixed;
mod handle;
#[doc(hidden)]
pub mod raw;
mod sealed;
mod waiters;
pub use fixed::identity::FixedIdentity;
pub use handle::Handle;
pub(in crate::driver::schedule::ready) use sealed::{
    Dispatch, Dynamic, Entry, FreeIndex, FreeLink, Kind, Layout, Pool, Reservation, Resolved,
    Slots, Table,
};
pub(super) enum Lane {}

#[derive(Clone, Copy)]
pub(in crate::driver) struct Access<'a, 'd> {
    _access: marker::PhantomData<fn(&'a ()) -> &'a ()>,
    _driver: marker::PhantomData<fn(driver::Reference<'d>) -> driver::Reference<'d>>,
}

impl<'d> Access<'_, 'd> {
    pub(in crate::driver) fn with<'a, R>(
        _driver: &'a driver::Reference<'d>,
        f: impl FnOnce(Access<'a, 'd>) -> R,
    ) -> R
    where
        'd: 'a,
    {
        f(Access {
            _access: marker::PhantomData,
            _driver: marker::PhantomData,
        })
    }
}

const _: () = assert!(mem::size_of::<Access<'static, 'static>>() == 0);

pub(in crate::driver::schedule::ready) enum TaskParent<'d> {
    Root,
    Task(completion::Wake<'d>),
    Stale,
}

pub mod completion;
#[doc(hidden)]
pub mod task;

/// A generation-checked address in a driver's local ready queue.
#[derive(Clone, Copy)]
pub struct Key<'d> {
    index: u32,
    epoch: u32,
    _arena: marker::PhantomData<fn(&'d Arena) -> &'d Arena>,
    _thread: o3::ThreadBound,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct FixedKey<'d>(Key<'d>);

#[repr(transparent)]
pub(crate) struct FixedRelease<'d> {
    slot: handles::FixedSlot,
    _arena: marker::PhantomData<fn(&'d Arena) -> &'d Arena>,
}

#[derive(Clone, Copy)]
pub(in crate::driver::schedule::ready) struct DynamicKey<'d> {
    index: FreeIndex,
    epoch: u32,
    _arena: marker::PhantomData<fn(&'d Arena) -> &'d Arena>,
    _thread: o3::ThreadBound,
}

impl Key<'static> {
    pub const NONE: Self = Self {
        index: u32::MAX,
        epoch: 0,
        _arena: marker::PhantomData,
        _thread: o3::ThreadBound::NEW,
    };
}

impl PartialEq for Key<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.epoch == other.epoch
    }
}

impl Eq for Key<'_> {}

const _: () = {
    assert!(mem::size_of::<FixedKey<'static>>() == mem::size_of::<Key<'static>>());
    assert!(mem::size_of::<FixedRelease<'static>>() == mem::size_of::<handles::FixedSlot>());
    assert!(mem::size_of::<DynamicKey<'static>>() == mem::size_of::<Key<'static>>());
    assert!(mem::align_of::<DynamicKey<'static>>() == mem::align_of::<Key<'static>>());
};

impl<'d> DynamicKey<'d> {
    fn new(index: FreeIndex, epoch: u32) -> Self {
        Self {
            index,
            epoch,
            _arena: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    fn key(self) -> Key<'d> {
        Key {
            index: self.index.raw(),
            epoch: self.epoch,
            _arena: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    fn index(self) -> FreeIndex {
        self.index
    }

    fn epoch(self) -> u32 {
        self.epoch
    }
}

impl<'d> FixedKey<'d> {
    pub(crate) fn new(index: u32, epoch: u32) -> Self {
        Self(Key {
            index,
            epoch,
            _arena: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        })
    }

    pub(crate) fn key(self) -> Key<'d> {
        self.0
    }

    pub(crate) fn index(self) -> u32 {
        self.0.index
    }

    pub(crate) fn epoch(self) -> u32 {
        self.0.epoch
    }
}

impl FixedRelease<'_> {
    pub(crate) fn slot(&self) -> handles::FixedSlot {
        self.slot
    }

    pub(crate) fn into_slot(self) -> handles::FixedSlot {
        self.slot
    }
}

/// A generation-checked driver readiness target that cannot name a task node.
#[derive(Clone, Copy)]
pub struct Target<'d> {
    driver: driver::Reference<'d>,
    key: Key<'d>,
}

impl PartialEq for Target<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.driver.same_driver(other.driver) && self.key == other.key
    }
}

impl Eq for Target<'_> {}

impl<'d> Target<'d> {
    pub(crate) fn new(driver: driver::Reference<'d>, key: Key<'d>) -> Self {
        Self { driver, key }
    }

    pub fn wake(self) {
        self.driver.ready().activate_ready(self.key);
    }

    #[doc(hidden)]
    pub fn into_key(self) -> Key<'d> {
        self.key
    }

    pub(super) fn into_parts(self) -> (driver::Reference<'d>, Key<'d>) {
        (self.driver, self.key)
    }
}

const _: () = {
    assert!(mem::size_of::<Target<'static>>() == 2 * mem::size_of::<usize>());
};

pub struct Slot<'d, Tag: route::Tag> {
    driver: driver::Reference<'d>,
    key: DynamicKey<'d>,
    target: marker::PhantomData<*mut Tag>,
}

impl<'d, Tag: route::Tag> Slot<'d, Tag> {
    fn new(driver: driver::Reference<'d>, key: DynamicKey<'d>) -> Self {
        Self {
            driver,
            key,
            target: marker::PhantomData,
        }
    }

    pub fn set_target(&self, target: route::Operation<'d, Tag>) {
        self.driver
            .ready()
            .arena()
            .set_target(self.key.key(), target.into_token());
    }

    pub fn activate(&self) {
        Access::with(&self.driver, |access| {
            self.driver.ready().arena().activate(access, self.key.key());
        });
    }

    pub fn key(&self) -> Key<'d> {
        self.key.key()
    }

    /// Returns the driver- and generation-branded root owned by this slot.
    pub fn target(&self) -> Target<'d> {
        Target::new(self.driver, self.key.key())
    }
}

impl<Tag: route::Tag> Drop for Slot<'_, Tag> {
    fn drop(&mut self) {
        self.driver.ready().arena().release(self.key);
    }
}

pub(in crate::driver) struct Arena {
    ready: set::Set,
    entries: Table,
    recv_credit_refs: Box<[cell::LocalRefCount]>,
    buffer_waiters: waiters::Waiters,
}

impl Arena {
    pub(in crate::driver) fn new(
        fixed: usize,
        dynamic: settings::ScheduleCapacity,
    ) -> io::Result<Box<Self>> {
        use collections::{BoxExt, BoxSliceExt};

        use crate::driver::route::{Epoch, FRAMEWORK, SlotIndex, Token};
        let layout = Layout::new(fixed, dynamic)?;
        let recv_credit_refs = BoxSliceExt::try_box_with(fixed, |_| cell::LocalRefCount::empty())
            .map_err(|error| io::Error::other(error.to_string()))?;

        let dummy = Token::new(FRAMEWORK, SlotIndex::ZERO, Epoch::INITIAL);
        let arena = Self {
            ready: set::Set::try_with_capacity(layout.capacity())?,
            entries: Table::try_new(layout, dummy)?,
            recv_credit_refs,
            buffer_waiters: waiters::Waiters::try_with_capacity(fixed)?,
        };
        Ok(BoxExt::try_box(arena)?)
    }

    pub(crate) fn claim_fixed(&self, slot: handles::FixedSlot) -> Option<FixedKey<'_>> {
        use crate::driver::route::{Epoch, FRAMEWORK, SlotIndex, Token};

        let index = slot.raw();
        debug_assert!(self.entries.slots.is_fixed(index as usize));
        let dummy = Token::new(FRAMEWORK, SlotIndex::ZERO, Epoch::INITIAL);
        let epoch = self.entries.slots.claim_fixed(index as usize, dummy)?;
        Some(FixedKey::new(index, epoch))
    }

    pub(crate) fn fixed_handle<'d>(
        &'d self,
        driver: driver::Reference<'d>,
        key: FixedKey<'d>,
    ) -> Handle<'d> {
        Handle::new(driver, key)
    }

    pub(crate) fn release_fixed<'d>(&self, key: FixedKey<'d>) -> Option<FixedRelease<'d>> {
        let transfer = self.buffer_waiters.retire(self, key);
        let released = self
            .entries
            .slots
            .release_fixed(key, |index| self.ready.remove(index));
        if transfer {
            self.buffer_waiters.wake(self);
        }
        if !released {
            return None;
        }
        Some(FixedRelease {
            slot: handles::FixedSlot::from_index(route::SlotIndex::from_bounded(key.index())),
            _arena: marker::PhantomData,
        })
    }

    pub(crate) fn make_slot<'d, Tag: route::Tag>(
        &'d self,
        driver: driver::Reference<'d>,
        target: route::Operation<'d, Tag>,
    ) -> io::Result<Slot<'d, Tag>> {
        self.make_slot_reserving(driver, target, 0)
    }

    pub(crate) fn make_slots<'d, Tag, I>(
        &'d self,
        driver: driver::Reference<'d>,
        targets: I,
    ) -> io::Result<Box<[Slot<'d, Tag>]>>
    where
        Tag: route::Tag,
        I: IntoIterator<Item = route::Operation<'d, Tag>>,
        I::IntoIter: ExactSizeIterator,
    {
        let targets = targets.into_iter();
        let requested = targets.len();
        let available = self.entries.pool.available();
        if requested > available {
            return Err(Pool::capacity_error(requested, available));
        }
        let mut slots: Vec<Slot<'d, Tag>> = collections::VecExt::try_vec_with_capacity(requested)?;
        for (index, target) in targets.enumerate() {
            let reserve = index
                .checked_add(1)
                .and_then(|used| requested.checked_sub(used))
                .ok_or_else(|| {
                    io::Error::other("dope: ready iterator exceeded its exact length")
                })?;
            slots.push(self.make_slot_reserving(driver, target, reserve)?);
        }
        if slots.len() != requested {
            return Err(io::Error::other(
                "dope: ready iterator ended before its exact length",
            ));
        }
        Ok(slots.into_boxed_slice())
    }

    pub(crate) fn make_slot_reserving<'d, Tag: route::Tag>(
        &'d self,
        driver: driver::Reference<'d>,
        target: route::Operation<'d, Tag>,
        reserve: usize,
    ) -> io::Result<Slot<'d, Tag>> {
        let key = self.entries.pool.reserve_dispatch(
            &self.entries.slots,
            reserve,
            target.into_token(),
        )?;
        Ok(Slot::new(driver, key))
    }

    fn set_target(&self, key: Key<'_>, target: route::Token) {
        let Some(resolved) = self.entries.slots.resolve(key) else {
            return;
        };
        let Some(dispatch): Option<Dispatch<'_>> = resolved.dispatch() else {
            return;
        };
        let current = dispatch.get();
        let transfer = if current.kind() == kind::RECV_BUFFER_WAITING {
            self.buffer_waiters.unlink_index(resolved.index());
            false
        } else {
            current.kind() == kind::RECV_BUFFER_GRANTED
        };
        if transfer {
            self.ready.remove(resolved.index());
        }
        dispatch.set(target);
        if transfer {
            self.buffer_waiters.wake(self);
        }
    }

    pub(crate) fn activate<'a, 'd>(&'a self, access: Access<'a, 'd>, key: Key<'d>)
    where
        'd: 'a,
    {
        let Some(resolved) = self.entries.slots.resolve(key) else {
            return;
        };
        match resolved.entry(access) {
            Entry::Vacant => {}
            Entry::Dispatch(_) => {
                self.ready.insert(resolved.index());
            }
            Entry::Task(node) if !task::raw::Binding::is_ready(node) => {
                self.ready.insert(resolved.index());
            }
            Entry::Task(_) => {}
        }
    }

    fn activate_dispatch(&self, key: Key<'_>) {
        let Some(resolved) = self.entries.slots.resolve(key) else {
            return;
        };
        if resolved.dispatch().is_some() {
            self.ready.insert(resolved.index());
        }
    }

    fn release<'d>(&'d self, key: DynamicKey<'d>) -> bool {
        self.entries
            .pool
            .release(&self.entries.slots, key, |index| self.ready.remove(index))
    }

    fn reclaim_task<'d>(&'d self, key: DynamicKey<'d>) -> (Option<Reservation<'d>>, bool) {
        self.entries
            .pool
            .reclaim_task(&self.entries.slots, key, |index| self.ready.remove(index))
    }

    pub(in crate::driver::schedule) fn drain<'a, 'd>(
        &'a self,
        access: Access<'a, 'd>,
        budget: &mut credits::Budget<'_, 'd, Lane>,
        mut activate: impl FnMut(route::Token),
    ) -> usize
    where
        'd: 'a,
    {
        let Some(mut ready) = self.ready.drain_batch() else {
            return 0;
        };
        let mut drained = 0;
        loop {
            let index = match budget.admit_with(|| ready.next()) {
                schedule::Admission::Item(index) => index,
                schedule::Admission::Empty => break,
                schedule::Admission::Exhausted => {
                    ready.pause();
                    break;
                }
            };
            drained += 1;
            match self.entries.slots.entry(index, access) {
                Entry::Vacant => {}
                Entry::Dispatch(target) => activate(target),
                Entry::Task(node) => {
                    if let Some(parent) = task::raw::Binding::activate(node) {
                        parent.wake();
                    }
                }
            }
        }
        drained
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    fn task_parent<'a, 'd>(&'a self, access: Access<'a, 'd>, key: Key<'d>) -> TaskParent<'d>
    where
        'd: 'a,
    {
        let Some(resolved) = self.entries.slots.resolve(key) else {
            return TaskParent::Stale;
        };
        match resolved.entry(access) {
            Entry::Dispatch(_) => TaskParent::Root,
            Entry::Task(node) => task::raw::Binding::parent(node)
                .map(TaskParent::Task)
                .unwrap_or(TaskParent::Stale),
            Entry::Vacant => TaskParent::Stale,
        }
    }
}
