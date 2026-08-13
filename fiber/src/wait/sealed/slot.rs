use std::{cell, pin};

use dope::core::driver::schedule::ready::completion;

use crate::{
    context,
    wait::{self, sealed},
};

/// A pinned, allocation-free one-waiter registration endpoint.
///
/// The target lifetime keeps the slot live through registration. Waiter drop
/// and slot wake both unlink in O(1).
///
/// ```compile_fail,E0277
/// use std::pin::Pin;
/// use dope_fiber::wait::Slot;
///
/// fn assert_pinned() {
///     let slot = Slot::new();
///     let _ = Pin::new(&slot);
/// }
/// ```
///
/// ```compile_fail
/// use std::pin::pin;
/// use dope::core::driver::schedule::ready::completion;
/// use dope_fiber::wait::{Slot, Waiter};
///
/// fn target_cannot_drop_first<'d>(wake: completion::Waker<'d>) {
///     let waiter = pin!(Waiter::new());
///     {
///         let slot = pin!(Slot::new());
///         assert!(slot.as_ref().try_register_completion(waiter.as_ref(), wake));
///     }
///     waiter.as_ref().unregister();
/// }
/// ```
#[pin_project::pin_project(!Unpin)]
pub struct Slot {
    waiter: cell::Cell<Option<sealed::SlotLink>>,
    _thread: o3::ThreadBound,
}

impl Slot {
    pub const fn new() -> Self {
        Self {
            waiter: cell::Cell::new(None),
            _thread: o3::ThreadBound::NEW,
        }
    }

    #[must_use]
    pub fn try_register<'target, 'poll, 'd>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> bool {
        self.try_register_completion(waiter, context.completion_waker())
    }

    #[doc(hidden)]
    pub fn try_register_completion<'target, 'd>(
        self: pin::Pin<&'target Self>,
        waiter: pin::Pin<&wait::Waiter<'target, 'd>>,
        wake: completion::Waker<'d>,
    ) -> bool {
        let slot = sealed::Target::slot(self);
        let wake = match waiter.registration.replace_wake(slot.0, wake) {
            Ok(()) => return true,
            Err(wake) => wake,
        };
        if self.waiter.get().is_some() {
            return false;
        }

        let (_, waiter) = waiter.vacate();
        let node = waiter.register_in_slot(slot, wake);
        self.waiter.set(Some(node));
        true
    }

    pub(super) fn detach(self: pin::Pin<&Self>, _waiter: sealed::SlotLink) {
        self.waiter.set(None);
    }

    fn pop(self: pin::Pin<&Self>, wake: bool) -> bool {
        let Some(waiter) = self.waiter.take() else {
            return false;
        };
        let registration = waiter.0.get().registration.take();
        if let Some(registration) = registration
            && wake
        {
            registration.wake.wake();
        }
        true
    }

    pub fn wake(self: pin::Pin<&Self>) {
        self.pop(true);
    }

    pub fn clear(self: pin::Pin<&Self>) {
        self.pop(false);
    }

    pub fn is_empty(&self) -> bool {
        self.waiter.get().is_none()
    }
}

impl Default for Slot {
    fn default() -> Self {
        Self::new()
    }
}
