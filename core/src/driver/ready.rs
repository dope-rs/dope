use std::cell::Cell;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::marker::PhantomData;
use std::ptr::NonNull;

use o3::cell::RegionToken;
use o3::collections::BatchSet;
use o3::marker::ThreadBound;

use super::DriverRef;
use super::token::{Epoch, ROUTE_FRAMEWORK, SlotIndex, Token};
use crate::io::fd::FdSlot;

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
///
/// This is a linear registration capability: it cannot be cloned or copied,
/// and `wake` consumes it. A rejected registration must return the value to
/// its caller instead of silently retaining a duplicate.
#[repr(transparent)]
pub struct CompletionWaker<'d>(RawCompletionWaker<'d>);

#[doc(hidden)]
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct CompletionKey<'d>(RawCompletionWaker<'d>);

#[derive(Clone, Copy)]
struct RawCompletionWaker<'d> {
    target: CompletionTarget<'d>,
    _region: PhantomData<fn(&'d RegionToken<'d>) -> &'d RegionToken<'d>>,
    _thread: ThreadBound,
}

impl fmt::Debug for CompletionWaker<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionWaker").finish_non_exhaustive()
    }
}

/// An owning slot for a linear [`CompletionWaker`].
///
/// The slot keeps the copyable representation private so owners can wake
/// without clearing the slot on the hot path. The surrounding operation state
/// remains responsible for deciding whether a wake is due.
#[doc(hidden)]
#[repr(transparent)]
pub struct CompletionSlot<'d> {
    wake: Cell<Option<RawCompletionWaker<'d>>>,
}

/// A proof-bearing sink for a completion handle retained after the current
/// operation.
///
/// # Safety
///
/// `register` must not expose the supplied handle through its output. If it
/// retains the handle, the implementing owner must ensure it cannot be invoked
/// after its source is invalidated. Deferred removal is valid only when every
/// path that could observe or invoke the handle first completes that removal.
/// Re-registration and every owner drop path must preserve this invariant.
#[doc(hidden)]
pub unsafe trait CompletionRegistrar<'d> {
    type Output;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output;
}

/// A completion registrar that also needs mutable executor-region access.
///
/// # Safety
///
/// The same requirements as [`CompletionRegistrar`] apply. The supplied
/// region token must not escape `register`.
#[doc(hidden)]
pub unsafe trait CompletionRegistrarWithRegion<'d> {
    type Output;

    fn register(self, wake: CompletionWaker<'d>, region: &mut RegionToken<'d>) -> Self::Output;
}

/// A callback target whose raw representation is valid for completion wakeups.
///
/// # Safety
///
/// `into_raw_parts` must return a callback that accepts exactly the returned
/// target. The target must remain valid until every registrar that receives
/// its completion handle has made that handle unobservable.
#[doc(hidden)]
pub unsafe trait CompletionCallback<'d> {
    fn into_raw_parts(self) -> (NonNull<()>, unsafe fn(NonNull<()>));
}

#[derive(Clone, Copy)]
enum CompletionTarget<'d> {
    Ready(DriverRef<'d>, ReadyKey<'d>),
    Callback(NonNull<()>, unsafe fn(NonNull<()>)),
}

impl<'d> CompletionWaker<'d> {
    pub fn from_ready(driver: DriverRef<'d>, key: ReadyKey<'d>) -> Self {
        Self(RawCompletionWaker {
            target: CompletionTarget::Ready(driver, key),
            _region: PhantomData,
            _thread: ThreadBound::NEW,
        })
    }

    /// Creates and delivers a callback-backed handle without exposing it.
    #[doc(hidden)]
    #[inline(always)]
    pub fn register_callback<C, R>(source: C, region: &RegionToken<'d>, registrar: R) -> R::Output
    where
        C: CompletionCallback<'d>,
        R: CompletionRegistrar<'d>,
    {
        let _ = region;
        let (target, callback) = source.into_raw_parts();
        registrar.register(Self(RawCompletionWaker {
            target: CompletionTarget::Callback(target, callback),
            _region: PhantomData,
            _thread: ThreadBound::NEW,
        }))
    }

    /// Creates and delivers a callback-backed handle with mutable region
    /// access, without exposing either capability.
    #[doc(hidden)]
    #[inline(always)]
    pub fn register_callback_with_region<C, R>(
        source: C,
        region: &mut RegionToken<'d>,
        registrar: R,
    ) -> R::Output
    where
        C: CompletionCallback<'d>,
        R: CompletionRegistrarWithRegion<'d>,
    {
        let (target, callback) = source.into_raw_parts();
        registrar.register(
            Self(RawCompletionWaker {
                target: CompletionTarget::Callback(target, callback),
                _region: PhantomData,
                _thread: ThreadBound::NEW,
            }),
            region,
        )
    }

    pub fn wake(self) {
        self.0.wake();
    }

    #[doc(hidden)]
    pub fn key(&self) -> CompletionKey<'d> {
        CompletionKey(self.0)
    }
}

impl RawCompletionWaker<'_> {
    fn same_target(&self, other: &Self) -> bool {
        match (self.target, other.target) {
            (
                CompletionTarget::Ready(left_driver, left_key),
                CompletionTarget::Ready(right_driver, right_key),
            ) => left_driver == right_driver && left_key == right_key,
            (
                CompletionTarget::Callback(left_target, left_callback),
                CompletionTarget::Callback(right_target, right_callback),
            ) => left_target == right_target && std::ptr::fn_addr_eq(left_callback, right_callback),
            _ => false,
        }
    }

    fn wake(self) {
        match self.target {
            CompletionTarget::Ready(driver, key) => driver.activate_ready(key),
            CompletionTarget::Callback(target, callback) => unsafe { callback(target) },
        }
    }
}

impl<'d> CompletionSlot<'d> {
    pub const fn empty() -> Self {
        Self {
            wake: Cell::new(None),
        }
    }

    pub fn set(&self, wake: CompletionWaker<'d>) {
        self.wake.set(Some(wake.0));
    }

    pub fn clear(&self) {
        self.wake.set(None);
    }

    pub fn is_empty(&self) -> bool {
        self.wake.get().is_none()
    }

    pub fn matches(&self, wake: &CompletionWaker<'d>) -> bool {
        self.matches_key(wake.key())
    }

    pub fn matches_key(&self, key: CompletionKey<'d>) -> bool {
        self.wake
            .get()
            .is_some_and(|current| current.same_target(&key.0))
    }

    pub fn clear_if(&self, key: CompletionKey<'d>) -> bool {
        if !self.matches_key(key) {
            return false;
        }
        self.clear();
        true
    }

    #[inline]
    pub fn take(&self) -> Option<CompletionWaker<'d>> {
        self.wake.take().map(CompletionWaker)
    }

    pub fn wake(&self) {
        if let Some(wake) = self.wake.get() {
            wake.wake();
        }
    }
}

#[derive(Clone, Copy)]
pub struct ReadyHandle<'d> {
    arena: &'d Arena,
    key: ReadyKey<'d>,
}

impl<'d> ReadyHandle<'d> {
    pub fn set_target(self, target: Token) {
        self.arena.set_target(self.key, target);
    }

    pub fn activate(self) {
        self.arena.activate(self.key);
    }

    pub fn key(self) -> ReadyKey<'d> {
        self.key
    }
}

pub struct ReadySlot<'d> {
    arena: &'d Arena,
    key: ReadyKey<'d>,
}

impl<'d> ReadySlot<'d> {
    fn new(arena: &'d Arena, index: u32) -> Self {
        arena.live[index as usize].set(true);
        Self {
            arena,
            key: ReadyKey {
                index,
                epoch: arena.epochs[index as usize].get(),
                _arena: PhantomData,
                _thread: ThreadBound::NEW,
            },
        }
    }

    pub fn set_target(&self, target: Token) {
        self.arena.set_target(self.key, target);
    }

    pub fn activate(&self) {
        self.arena.activate(self.key);
    }

    pub fn key(&self) -> ReadyKey<'d> {
        self.key
    }
}

impl Drop for ReadySlot<'_> {
    fn drop(&mut self) {
        self.arena.release(self.key);
    }
}

pub(super) struct Arena {
    fixed: usize,
    ready: BatchSet,
    targets: Box<[Cell<Token>]>,
    epochs: Box<[Cell<u32>]>,
    live: Box<[Cell<bool>]>,
    next_free: Box<[Cell<u32>]>,
    free: Cell<u32>,
    free_len: Cell<usize>,
}

impl Arena {
    pub(super) fn new(fixed: usize, dynamic: usize) -> Result<Box<Self>> {
        let capacity = fixed
            .checked_add(dynamic)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "dope: ready capacity overflow"))?;
        if capacity > u32::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: ready capacity exceeds u32",
            ));
        }

        let dummy = Token::new(ROUTE_FRAMEWORK, SlotIndex::ZERO, Epoch::INITIAL);
        Ok(Box::new(Self {
            fixed,
            ready: BatchSet::with_capacity(capacity),
            targets: (0..capacity).map(|_| Cell::new(dummy)).collect(),
            epochs: (0..capacity).map(|_| Cell::new(0)).collect(),
            live: (0..capacity)
                .map(|index| Cell::new(index < fixed))
                .collect(),
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
        }))
    }

    fn valid(&self, key: ReadyKey<'_>) -> bool {
        let index = key.index as usize;
        self.live.get(index).is_some_and(Cell::get) && self.epochs[index].get() == key.epoch
    }

    pub(crate) fn fixed_slot(&self, slot: FdSlot) -> ReadyHandle<'_> {
        let index = slot.raw();
        debug_assert!((index as usize) < self.fixed);
        ReadyHandle {
            arena: self,
            key: ReadyKey {
                index,
                epoch: 0,
                _arena: PhantomData,
                _thread: ThreadBound::NEW,
            },
        }
    }

    pub(crate) fn fd_slot(&self, raw: u32) -> Option<FdSlot> {
        if (raw as usize) >= self.fixed {
            return None;
        }
        FdSlot::try_from_raw(raw)
    }

    pub(crate) fn make_slot(&self, target: Token) -> Result<ReadySlot<'_>> {
        self.make_slot_reserving(target, 0)
    }

    pub(crate) fn make_slots<I>(&self, targets: I) -> Result<Box<[ReadySlot<'_>]>>
    where
        I: IntoIterator<Item = Token>,
        I::IntoIter: ExactSizeIterator,
    {
        let targets = targets.into_iter();
        let requested = targets.len();
        let available = self.free_len.get();
        if requested > available {
            return Err(Self::capacity_error(requested, available));
        }
        targets
            .enumerate()
            .map(|(index, target)| self.make_slot_reserving(target, requested - index - 1))
            .collect()
    }

    pub(crate) fn make_slot_reserving(
        &self,
        target: Token,
        reserve: usize,
    ) -> Result<ReadySlot<'_>> {
        if self.free_len.get() <= reserve {
            return Err(Self::capacity_error(1, self.free_len.get()));
        }
        let index = self.free.get();
        if index == NIL {
            return Err(Self::capacity_error(1, 0));
        }
        self.free.set(self.next_free[index as usize].get());
        self.free_len.set(self.free_len.get() - 1);
        self.targets[index as usize].set(target);
        Ok(ReadySlot::new(self, index))
    }

    fn capacity_error(requested: usize, available: usize) -> Error {
        Error::new(
            ErrorKind::WouldBlock,
            format!(
                "dope: dynamic ready capacity exhausted: requested {requested}, available {available}"
            ),
        )
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

    pub(crate) fn arm_recv_credit(&self, key: ReadyKey<'_>, target: Token) -> bool {
        use super::token::kind::RECV_CREDIT_HELD;

        if !self.valid(key) {
            return false;
        }
        let current = self.targets[key.index as usize].get();
        if !current.same_target(target) {
            return false;
        }
        self.targets[key.index as usize].set(target.with_kind(RECV_CREDIT_HELD));
        true
    }

    pub(crate) fn release_recv_credit(&self, key: ReadyKey<'_>, target: Token) {
        use super::token::kind::{RECV_CREDIT_HELD, RECV_CREDIT_RELEASED};

        if !self.valid(key) {
            return;
        }
        let current = self.targets[key.index as usize].get();
        if current != target.with_kind(RECV_CREDIT_HELD) {
            return;
        }
        self.targets[key.index as usize].set(target.with_kind(RECV_CREDIT_RELEASED));
        self.ready.insert(key.index as usize);
    }

    pub(crate) fn take_recv_credit(&self, key: ReadyKey<'_>, target: Token) -> bool {
        use super::token::kind::RECV_CREDIT_RELEASED;

        if !self.valid(key) {
            return false;
        }
        let current = self.targets[key.index as usize].get();
        if current != target.with_kind(RECV_CREDIT_RELEASED) {
            return false;
        }
        self.targets[key.index as usize].set(target.with_kind(0));
        true
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

#[cfg(test)]
mod tests {
    use super::Arena;
    use crate::driver::token::kind::{RECV_CREDIT_HELD, RECV_CREDIT_RELEASED};
    use crate::driver::token::{Epoch, SlotIndex, Token};
    use crate::io::fd::FdSlot;

    fn target(epoch: Epoch) -> Token {
        Token::new(7, SlotIndex::ZERO, epoch)
    }

    #[test]
    fn application_wake_does_not_release_held_receive_credit() {
        let arena = Arena::new(1, 0).unwrap();
        let handle = arena.fixed_slot(FdSlot::try_from_raw(0).unwrap());
        let target = target(Epoch::INITIAL);
        handle.set_target(target);

        assert!(arena.arm_recv_credit(handle.key(), target));
        handle.activate();
        let mut activated = Vec::new();
        arena.drain(|token| activated.push(token));
        assert_eq!(activated, [target.with_kind(RECV_CREDIT_HELD)]);
        assert!(!arena.take_recv_credit(handle.key(), target));

        arena.release_recv_credit(handle.key(), target);
        activated.clear();
        arena.drain(|token| activated.push(token));
        assert_eq!(activated, [target.with_kind(RECV_CREDIT_RELEASED)]);
        assert!(arena.take_recv_credit(handle.key(), target));

        handle.activate();
        activated.clear();
        arena.drain(|token| activated.push(token));
        assert_eq!(activated, [target]);
    }

    #[test]
    fn stale_guard_cannot_release_credit_for_a_reused_connection() {
        let arena = Arena::new(1, 0).unwrap();
        let handle = arena.fixed_slot(FdSlot::try_from_raw(0).unwrap());
        let old = target(Epoch::INITIAL);
        let new = target(Epoch::INITIAL.next().unwrap());
        handle.set_target(old);
        assert!(arena.arm_recv_credit(handle.key(), old));

        handle.set_target(new);
        arena.release_recv_credit(handle.key(), old);
        assert!(!arena.has_ready());

        handle.activate();
        let mut activated = Vec::new();
        arena.drain(|token| activated.push(token));
        assert_eq!(activated, [new]);
    }
}
