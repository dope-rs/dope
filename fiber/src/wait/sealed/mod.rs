use std::{cell, marker, mem, pin, ptr};

use dope::core::driver::{
    self,
    schedule::{self, ready::completion},
};
use o3::{self};

use crate::context;

mod facades;
mod links;
mod slot;
pub use facades::{Queue, Slots, Table, WakeStatus};
pub use slot::Slot;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct RegistryLink(links::WaiterLink);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct SlotLink(links::WaiterLink);

const SLOT_TAG: usize = 1;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct Target(ptr::NonNull<()>);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct RegistryTarget(Target);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct SlotTarget(Target);

enum DecodedTarget {
    Registry(pin::Pin<&'static Registry<'static>>),
    Slot(pin::Pin<&'static Slot>),
}

struct ActiveRegistration<'d> {
    target: Target,
    wake: completion::Waker<'d>,
}

/// Stores one registration with `target` as its initialized-state tag.
/// `wake` is initialized before the tag and moved only after clearing it, so
/// the private API cannot represent a target without its linear waker.
struct Registration<'d> {
    target: cell::Cell<Option<Target>>,
    wake: cell::Cell<mem::MaybeUninit<completion::Waker<'d>>>,
}

impl<'d> Registration<'d> {
    const fn vacant() -> Self {
        Self {
            target: cell::Cell::new(None),
            wake: cell::Cell::new(mem::MaybeUninit::uninit()),
        }
    }

    fn target(&self) -> Option<Target> {
        self.target.get()
    }

    fn replace_wake(
        &self,
        target: Target,
        wake: completion::Waker<'d>,
    ) -> Result<(), completion::Waker<'d>> {
        if self.target.get() != Some(target) {
            return Err(wake);
        }

        let previous = self.wake.replace(mem::MaybeUninit::new(wake));
        // SAFETY: a matching non-empty target is the initialization tag for
        // wake, and the replacement leaves the registration initialized.
        let _previous = unsafe { previous.assume_init() };
        Ok(())
    }

    fn take(&self) -> Option<ActiveRegistration<'d>> {
        let target = self.target.take()?;
        let wake = self.wake.replace(mem::MaybeUninit::uninit());
        // SAFETY: `target` was Some, so wake was initialized. Clearing the tag
        // before moving wake out leaves the registration vacant even if the
        // returned value is immediately dropped.
        let wake = unsafe { wake.assume_init() };
        Some(ActiveRegistration { target, wake })
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        if self.target.get_mut().take().is_some() {
            // SAFETY: a non-empty target is the initialization tag for wake.
            unsafe { self.wake.get_mut().assume_init_drop() };
        }
    }
}

impl Target {
    fn registry(value: pin::Pin<&Registry<'_>>) -> RegistryTarget {
        RegistryTarget(Self(ptr::NonNull::from(value.get_ref()).cast()))
    }

    fn slot(value: pin::Pin<&Slot>) -> SlotTarget {
        let pointer = ptr::NonNull::from(value.get_ref()).cast::<()>();
        SlotTarget(Self(pointer.map_addr(|address| address | SLOT_TAG)))
    }

    fn decode(self) -> DecodedTarget {
        let is_slot = self.0.addr().get() & SLOT_TAG != 0;
        let pointer = self.0.map_addr(|address| {
            // SAFETY: Registry and Slot are both aligned to at least two
            // bytes. Clearing the private low tag therefore recovers the
            // original non-null address.
            unsafe {
                use std::num;
                num::NonZeroUsize::new_unchecked(address.get() & !SLOT_TAG)
            }
        });
        unsafe {
            use std::pin::Pin;
            if is_slot {
                DecodedTarget::Slot(Pin::new_unchecked(pointer.cast::<Slot>().as_ref()))
            } else {
                DecodedTarget::Registry(Pin::new_unchecked(
                    pointer.cast::<Registry<'static>>().as_ref(),
                ))
            }
        }
    }
}

/// A driver-branded, bounded, allocation-free FIFO.
///
/// A registered waiter borrows its target for its own invariant target
/// lifetime, so safe code cannot drop this registry while a link is live.
///
/// ```compile_fail
/// use std::pin::pin;
/// use dope::core::driver::{self, schedule::ready::completion};
/// use dope_fiber::wait::{Registry, Waiter};
///
/// fn target_cannot_drop_first<'d>(
///     driver: driver::Reference<'d>,
///     wake: completion::Waker<'d>,
/// ) {
///     let waiter = pin!(Waiter::new());
///     {
///         let registry = pin!(Registry::with_capacity(driver, 1));
///         assert!(registry.as_ref().try_register_completion(waiter.as_ref(), wake));
///     }
///     waiter.as_ref().unregister();
/// }
/// ```
///
/// ```compile_fail
/// use std::pin::Pin;
/// use dope::core::driver::schedule::ready::completion;
/// use dope_fiber::wait::{Registry, Waiter};
///
/// fn cross_driver<'target, 'left, 'right>(
///     registry: Pin<&'target Registry<'left>>,
///     waiter: Pin<&Waiter<'target, 'left>>,
///     foreign: completion::Waker<'right>,
/// ) {
///     registry.try_register_completion(waiter, foreign);
/// }
/// ```
///
/// ```compile_fail
/// use std::pin::Pin;
/// use dope::core::driver::schedule;
/// use dope_fiber::wait::Registry;
///
/// fn cross_driver_work<'left, 'right>(
///     registry: Pin<&Registry<'left>>,
///     foreign: schedule::Application<'_, 'right>,
/// ) {
///     registry.wake(foreign);
/// }
/// ```
#[pin_project::pin_project(!Unpin)]
pub struct Registry<'d> {
    head: cell::Cell<Option<RegistryLink>>,
    tail: cell::Cell<Option<RegistryLink>>,
    len: cell::Cell<usize>,
    capacity: usize,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _thread: o3::ThreadBound,
}

pub struct Waiter<'target, 'd> {
    registration: Registration<'d>,
    previous: cell::Cell<Option<RegistryLink>>,
    next: cell::Cell<Option<RegistryLink>>,
    _target: marker::PhantomData<cell::Cell<&'target ()>>,
    _pin: marker::PhantomPinned,
    _thread: o3::ThreadBound,
}

/// Proof that a pinned waiter is detached and its registration storage is
/// vacant. Only [`Waiter::vacate`] can construct this token.
struct VacantWaiter<'a, 'target, 'd> {
    waiter: pin::Pin<&'a Waiter<'target, 'd>>,
}

impl<'a, 'target, 'd> VacantWaiter<'a, 'target, 'd> {
    fn register_in_registry(
        self,
        target: RegistryTarget,
        wake: completion::Waker<'d>,
    ) -> RegistryLink {
        self.waiter
            .registration
            .wake
            .set(mem::MaybeUninit::new(wake));
        self.waiter.registration.target.set(Some(target.0));
        RegistryLink(links::WaiterLinks::from_waiter(self.waiter))
    }

    fn register_in_slot(self, target: SlotTarget, wake: completion::Waker<'d>) -> SlotLink {
        self.waiter
            .registration
            .wake
            .set(mem::MaybeUninit::new(wake));
        self.waiter.registration.target.set(Some(target.0));
        SlotLink(links::WaiterLinks::from_waiter(self.waiter))
    }
}

impl<'d> Registry<'d> {
    pub const fn with_capacity(_driver: driver::Reference<'d>, capacity: usize) -> Self {
        Self {
            head: cell::Cell::new(None),
            tail: cell::Cell::new(None),
            len: cell::Cell::new(0),
            capacity,
            _driver: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    fn contains<'target>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&Waiter<'target, 'd>>,
    ) -> bool {
        waiter.registration.target() == Some(Target::registry(self).0)
    }

    pub fn can_register<'target>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&Waiter<'target, 'd>>,
    ) -> bool {
        self.contains(waiter) || self.len.get() < self.capacity
    }

    #[must_use]
    pub fn try_register<'target, 'poll>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> bool {
        self.try_register_completion(waiter, context.completion_waker())
    }

    #[doc(hidden)]
    pub fn try_register_completion<'target>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&Waiter<'target, 'd>>,
        wake: completion::Waker<'d>,
    ) -> bool {
        let queue = Target::registry(self);
        let wake = match waiter.registration.replace_wake(queue.0, wake) {
            Ok(()) => return true,
            Err(wake) => wake,
        };
        if self.len.get() == self.capacity {
            return false;
        }

        let (_, waiter) = waiter.vacate();
        let previous = self.tail.get();
        waiter.waiter.previous.set(previous);
        let node = waiter.register_in_registry(queue, wake);
        if let Some(previous) = previous {
            previous.0.get().next.set(Some(node));
        } else {
            self.head.set(Some(node));
        }
        self.tail.set(Some(node));
        self.len.set(self.len.get() + 1);
        true
    }

    fn detach(self: pin::Pin<&Self>, link: RegistryLink) {
        let waiter = link.0.get();
        let previous = waiter.previous.take();
        let next = waiter.next.take();
        if let Some(previous) = previous {
            previous.0.get().next.set(next);
        } else {
            self.head.set(next);
        }
        if let Some(next) = next {
            next.0.get().previous.set(previous);
        } else {
            self.tail.set(previous);
        }
        self.len.set(self.len.get() - 1);
    }

    fn pop_next(self: pin::Pin<&Self>, wake: bool) -> bool {
        let Some(node) = self.head.get() else {
            return false;
        };
        let registration = node.0.get().registration.take();
        self.detach(node);
        if let Some(registration) = registration
            && wake
        {
            registration.wake.wake();
        }
        true
    }

    /// Wakes registered waiters until the queue is empty or this turn's
    /// application budget is exhausted.
    pub fn wake(self: pin::Pin<&Self>, work: schedule::Application<'_, 'd>) -> WakeStatus {
        while !self.is_empty() {
            if !work.take() {
                return WakeStatus::Pending;
            }
            let popped = self.pop_next(true);
            debug_assert!(popped);
        }
        WakeStatus::Complete
    }

    /// Wakes at most one waiter, consuming exactly one application credit.
    /// An empty queue consumes no credit.
    pub fn wake_one(self: pin::Pin<&Self>, work: schedule::Application<'_, 'd>) -> WakeStatus {
        if self.is_empty() {
            return WakeStatus::Complete;
        }
        if !work.take() {
            return WakeStatus::Pending;
        }
        let popped = self.pop_next(true);
        debug_assert!(popped);
        WakeStatus::Complete
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'target, 'd> Waiter<'target, 'd> {
    pub const fn new() -> Self {
        Self {
            registration: Registration::vacant(),
            previous: cell::Cell::new(None),
            next: cell::Cell::new(None),
            _target: marker::PhantomData,
            _pin: marker::PhantomPinned,
            _thread: o3::ThreadBound::NEW,
        }
    }

    fn vacate<'a>(self: pin::Pin<&'a Self>) -> (bool, VacantWaiter<'a, 'target, 'd>) {
        let Some(registration) = self.registration.take() else {
            return (false, VacantWaiter { waiter: self });
        };
        let waiter = links::WaiterLinks::from_waiter(self);
        match registration.target.decode() {
            DecodedTarget::Registry(queue) => queue.detach(RegistryLink(waiter)),
            DecodedTarget::Slot(slot) => slot.detach(SlotLink(waiter)),
        }
        (true, VacantWaiter { waiter: self })
    }

    pub fn unregister(self: pin::Pin<&Self>) -> bool {
        self.vacate().0
    }

    pub fn is_registered(&self) -> bool {
        self.registration.target().is_some()
    }
}

impl Default for Waiter<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Waiter<'_, '_> {
    fn drop(&mut self) {
        let Some(registration) = self.registration.take() else {
            return;
        };
        // SAFETY: Drop cannot move the pinned waiter before unlinking it.
        let waiter = links::WaiterLinks::from_waiter(unsafe { pin::Pin::new_unchecked(&*self) });
        match registration.target.decode() {
            DecodedTarget::Registry(queue) => queue.detach(RegistryLink(waiter)),
            DecodedTarget::Slot(slot) => slot.detach(SlotLink(waiter)),
        }
    }
}

const _: () = assert!(std::mem::align_of::<Registry<'static>>() >= 2);
const _: () = assert!(std::mem::align_of::<Slot>() >= 2);
const _: () = assert!(std::mem::size_of::<Slot>() == std::mem::size_of::<usize>());
const _: () =
    assert!(std::mem::size_of::<RegistryLink>() == std::mem::size_of::<links::WaiterLink>());
const _: () = assert!(std::mem::size_of::<SlotLink>() == std::mem::size_of::<links::WaiterLink>());
const _: () = assert!(std::mem::size_of::<RegistryTarget>() == std::mem::size_of::<Target>());
const _: () = assert!(std::mem::size_of::<SlotTarget>() == std::mem::size_of::<Target>());
const _: () = assert!(
    std::mem::size_of::<Registration<'static>>()
        == std::mem::size_of::<Target>() + std::mem::size_of::<completion::Waker<'static>>()
);
