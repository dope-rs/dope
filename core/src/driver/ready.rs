use std::cell::Cell;
use std::io::{Error, ErrorKind, Result};
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::ptr::NonNull;

use o3::collections::BatchSet;
use o3::marker::ThreadBound;

use crate::io::fd::FdSlot;

use super::DriverRef;
use super::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};

const NIL: u32 = u32::MAX;

/// A generation-checked address in a driver's local ready queue.
#[derive(Clone, Copy)]
pub struct ReadyKey<'d> {
    index: u32,
    epoch: u32,
    _arena: PhantomData<&'d Arena>,
    _thread: ThreadBound,
}

impl ReadyKey<'static> {
    pub const NONE: Self = Self {
        index: NIL,
        epoch: 0,
        _arena: PhantomData,
        _thread: ThreadBound::NEW,
    };
}

impl PartialEq for ReadyKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.epoch == other.epoch
    }
}

impl Eq for ReadyKey<'_> {}

/// A driver-scoped, type-erased wake target for an in-flight operation.
///
/// Unlike [`ReadyKey`], this preserves hierarchical task wakeups. A completion
/// can therefore wake the exact child task that registered the operation,
/// rather than only waking the root runtime task.
#[derive(Clone, Copy)]
pub struct CompletionWaker<'d> {
    target: CompletionTarget<'d>,
    _thread: ThreadBound,
}

#[derive(Clone, Copy)]
enum CompletionTarget<'d> {
    Ready(DriverRef<'d>, ReadyKey<'d>),
    Callback(NonNull<()>, unsafe fn(NonNull<()>)),
}

impl<'d> CompletionWaker<'d> {
    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self {
            target: CompletionTarget::Ready(driver, key),
            _thread: ThreadBound::NEW,
        }
    }

    /// # Safety
    ///
    /// `target` must remain valid for every call to `wake` while this handle is
    /// live, and `callback` must accept that exact target.
    pub unsafe fn from_callback(target: NonNull<()>, callback: unsafe fn(NonNull<()>)) -> Self {
        Self {
            target: CompletionTarget::Callback(target, callback),
            _thread: ThreadBound::NEW,
        }
    }

    #[inline]
    pub fn wake(self) {
        match self.target {
            CompletionTarget::Ready(driver, key) => driver.activate_ready(key),
            CompletionTarget::Callback(target, callback) => unsafe { callback(target) },
        }
    }
}

pub struct ReadySlot<'d> {
    arena: NonNull<Arena>,
    key: ReadyKey<'d>,
    owned: bool,
    _pin: PhantomPinned,
}

impl<'d> ReadySlot<'d> {
    fn new(arena: &'d Arena, index: u32, owned: bool) -> Self {
        arena.live[index as usize].set(true);
        Self {
            arena: NonNull::from(arena),
            key: ReadyKey {
                index,
                epoch: arena.epochs[index as usize].get(),
                _arena: PhantomData,
                _thread: ThreadBound::NEW,
            },
            owned,
            _pin: PhantomPinned,
        }
    }

    pub fn get(slots: Pin<&[Self]>, index: usize) -> Option<Pin<&Self>> {
        let slot = slots.get_ref().get(index)?;
        Some(unsafe { Pin::new_unchecked(slot) })
    }

    pub fn set_target(self: Pin<&Self>, target: Token) {
        unsafe { self.arena.as_ref() }.set_target(self.key, target);
    }

    #[inline]
    pub fn activate(self: Pin<&Self>) {
        unsafe { self.arena.as_ref() }.activate(self.key);
    }

    pub fn key(self: Pin<&Self>) -> ReadyKey<'d> {
        ReadyKey {
            index: self.key.index,
            epoch: self.key.epoch,
            _arena: PhantomData,
            _thread: ThreadBound::NEW,
        }
    }
}

impl Drop for ReadySlot<'_> {
    fn drop(&mut self) {
        if self.owned {
            unsafe { self.arena.as_ref() }.release(self.key);
        }
    }
}

pub(super) struct Arena {
    /// Drops before the arena fields referenced by every fixed slot.
    fixed: Pin<Box<[ReadySlot<'static>]>>,
    ready: BatchSet,
    targets: Box<[Cell<Token>]>,
    epochs: Box<[Cell<u32>]>,
    live: Box<[Cell<bool>]>,
    next_free: Box<[Cell<u32>]>,
    free: Cell<u32>,
    free_len: Cell<usize>,
    _pin: PhantomPinned,
}

impl Arena {
    pub(super) fn new(fixed: usize, dynamic: usize) -> Result<Pin<Box<Self>>> {
        let capacity = fixed
            .checked_add(dynamic)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "dope: ready capacity overflow"))?;
        if capacity > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: ready capacity exceeds u32",
            ));
        }

        let dummy = Token::new(ROUTE_FRAMEWORK, SlotIndex::new(0), Epoch::INITIAL);
        let mut arena = Box::pin(Self {
            fixed: Box::into_pin(Vec::new().into_boxed_slice()),
            ready: BatchSet::with_capacity(capacity),
            targets: (0..capacity).map(|_| Cell::new(dummy)).collect(),
            epochs: (0..capacity).map(|_| Cell::new(0)).collect(),
            live: (0..capacity).map(|_| Cell::new(false)).collect(),
            next_free: (0..capacity)
                .map(|index| {
                    let next = if index + 1 < capacity {
                        index as u32 + 1
                    } else {
                        NIL
                    };
                    Cell::new(next)
                })
                .collect(),
            free: Cell::new(if fixed < capacity { fixed as u32 } else { NIL }),
            free_len: Cell::new(capacity - fixed),
            _pin: PhantomPinned,
        });

        let arena_ref: &'static Arena = unsafe { &*(arena.as_ref().get_ref() as *const Arena) };
        let slots: Box<[ReadySlot<'static>]> = (0..fixed)
            .map(|index| ReadySlot::new(arena_ref, index as u32, false))
            .collect();
        unsafe { arena.as_mut().get_unchecked_mut() }.fixed = Box::into_pin(slots);
        Ok(arena)
    }

    fn valid(&self, key: ReadyKey<'_>) -> bool {
        let index = key.index as usize;
        self.live.get(index).is_some_and(Cell::get) && self.epochs[index].get() == key.epoch
    }

    pub(crate) fn slot<'d>(&'d self, slot: FdSlot) -> Pin<&'d ReadySlot<'d>> {
        let slot = &self.fixed[slot.raw() as usize];
        unsafe { Pin::new_unchecked(slot) }
    }

    pub(crate) fn make_slot<'d>(&'d self, target: Token) -> ReadySlot<'d> {
        self.try_make_slot(target)
            .expect("dope: dynamic ready slots exhausted")
    }

    pub(crate) fn try_make_slot<'d>(&'d self, target: Token) -> Option<ReadySlot<'d>> {
        self.try_make_slot_reserving(target, 0)
    }

    pub(crate) fn try_make_slot_reserving<'d>(
        &'d self,
        target: Token,
        reserve: usize,
    ) -> Option<ReadySlot<'d>> {
        if self.free_len.get() <= reserve {
            return None;
        }
        let index = self.free.get();
        if index == NIL {
            return None;
        }
        self.free.set(self.next_free[index as usize].get());
        self.free_len.set(self.free_len.get() - 1);
        self.targets[index as usize].set(target);
        Some(ReadySlot::new(self, index, true))
    }

    fn set_target(&self, key: ReadyKey<'_>, target: Token) {
        if self.valid(key) {
            self.targets[key.index as usize].set(target);
        }
    }

    pub(crate) fn activate(&self, key: ReadyKey<'_>) {
        if self.valid(key) {
            self.ready.insert(key.index as usize);
        }
    }

    fn release(&self, key: ReadyKey<'_>) {
        if !self.valid(key) {
            return;
        }
        let index = key.index as usize;
        self.live[index].set(false);
        self.ready.remove(index);
        let Some(epoch) = key.epoch.checked_add(1) else {
            self.next_free[index].set(NIL);
            return;
        };
        self.epochs[index].set(epoch);
        self.next_free[index].set(self.free.get());
        self.free.set(key.index);
        self.free_len.set(self.free_len.get() + 1);
    }

    pub(crate) fn drain(&self, mut activate: impl FnMut(Token)) {
        let Some(mut ready) = self.ready.drain_batch() else {
            return;
        };
        for index in &mut ready {
            if self.live[index].get() {
                activate(self.targets[index].get());
            }
        }
    }

    pub(crate) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }
}
