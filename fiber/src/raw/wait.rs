use std::cell::Cell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::process::abort;
use std::ptr::NonNull;

use dope::driver::ready::{CompletionSlot, CompletionWaker};
use o3::marker::ThreadBound;
use pin_project::{pin_project, pinned_drop};

use crate::raw::link::{PinnedLink, StableLinkSource};
use crate::raw::pinned_slice;
use crate::raw::task::{CompletionOwner, CompletionRegistrar, Context};

type WaiterLink = PinnedLink<Waiter<'static>>;

struct WaitLinkSource<T>(NonNull<T>);

// SAFETY: this private source is constructed only for a live bidirectional
// registration. Either endpoint's Drop revokes both directions first.
unsafe impl<T> StableLinkSource<T> for WaitLinkSource<T> {
    fn pointer(self) -> NonNull<T> {
        self.0
    }
}

// SAFETY: the pinned waiter owns the completion handle. Waiter Drop unlinks
// it, and queue Drop detaches every waiter before either endpoint disappears.
unsafe impl<'d> CompletionRegistrar<'d> for CompletionOwner<(Pin<&WaitQueue>, Pin<&Waiter<'d>>)> {
    type Output = bool;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (queue, waiter) = self.0;
        queue.try_register_completion(waiter, wake)
    }
}

/// A bounded, allocation-free FIFO whose pinned endpoints unlink on drop.
#[pin_project(PinnedDrop, !Unpin)]
pub struct WaitQueue {
    head: Cell<Option<WaiterLink>>,
    tail: Cell<Option<WaiterLink>>,
    len: Cell<usize>,
    capacity: usize,
    _thread: ThreadBound,
}

pub struct Waiter<'d> {
    queue: Cell<Option<PinnedLink<WaitQueue>>>,
    previous: Cell<Option<WaiterLink>>,
    next: Cell<Option<WaiterLink>>,
    wake: CompletionSlot<'d>,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

impl WaitQueue {
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            head: Cell::new(None),
            tail: Cell::new(None),
            len: Cell::new(0),
            capacity,
            _thread: ThreadBound::NEW,
        }
    }

    /// Projects one queue from a pinned slice.
    pub fn pinned(queues: Pin<&[Self]>, index: usize) -> Option<Pin<&Self>> {
        pinned_slice::get(queues, index)
    }

    fn contains<'d>(self: Pin<&Self>, waiter: Pin<&Waiter<'d>>) -> bool {
        let queue = PinnedLink::from_stable(WaitLinkSource(NonNull::from(self.get_ref())));
        waiter.queue.get() == Some(queue)
    }

    pub fn can_register<'d>(self: Pin<&Self>, waiter: Pin<&Waiter<'d>>) -> bool {
        self.contains(waiter) || self.len.get() < self.capacity
    }

    #[must_use]
    pub fn try_register<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        context.register_completion(CompletionOwner((self, waiter)))
    }

    #[doc(hidden)]
    pub fn try_register_completion<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d>>,
        wake: CompletionWaker<'d>,
    ) -> bool {
        if self.contains(waiter) {
            waiter.wake.set(wake);
            return true;
        }
        if self.len.get() == self.capacity {
            return false;
        }

        waiter.unregister();
        debug_assert!(waiter.previous.get().is_none());
        debug_assert!(waiter.next.get().is_none());

        let queue = PinnedLink::from_stable(WaitLinkSource(NonNull::from(self.get_ref())));
        let node = PinnedLink::from_stable(WaitLinkSource(
            NonNull::from(waiter.get_ref()).cast::<Waiter<'static>>(),
        ));
        let previous = self.tail.get();
        waiter.queue.set(Some(queue));
        waiter.previous.set(previous);
        waiter.wake.set(wake);
        if let Some(previous) = previous {
            previous.get().next.set(Some(node));
        } else {
            self.head.set(Some(node));
        }
        self.tail.set(Some(node));
        self.len.set(self.len.get() + 1);
        true
    }

    fn unlink<'d>(self: Pin<&Self>, waiter: PinnedLink<Waiter<'d>>) -> Option<CompletionWaker<'d>> {
        let waiter = waiter.get();
        let queue = PinnedLink::from_stable(WaitLinkSource(NonNull::from(self.get_ref())));
        if waiter.queue.get() != Some(queue) {
            return None;
        }

        let previous = waiter.previous.take();
        let next = waiter.next.take();
        if let Some(previous) = previous {
            previous.get().next.set(next);
        } else {
            self.head.set(next);
        }
        if let Some(next) = next {
            next.get().previous.set(previous);
        } else {
            self.tail.set(previous);
        }
        waiter.queue.set(None);
        self.len.set(self.len.get() - 1);
        waiter.wake.take()
    }

    fn pop_next(self: Pin<&Self>, wake: bool) -> bool {
        let Some(node) = self.head.get() else {
            return false;
        };
        let Some(waker) = self.unlink(node) else {
            abort();
        };
        if wake {
            waker.wake();
        }
        true
    }

    pub fn wake(self: Pin<&Self>) {
        while self.pop_next(true) {}
    }

    pub fn wake_one(self: Pin<&Self>) {
        self.pop_next(true);
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[pinned_drop]
impl PinnedDrop for WaitQueue {
    fn drop(self: Pin<&mut Self>) {
        while self.as_ref().pop_next(false) {}
    }
}

impl<'d> Waiter<'d> {
    pub const fn new() -> Self {
        Self {
            queue: Cell::new(None),
            previous: Cell::new(None),
            next: Cell::new(None),
            wake: CompletionSlot::empty(),
            _pin: PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn unregister(self: Pin<&Self>) -> bool {
        let Some(queue) = self.queue.get() else {
            return false;
        };
        let waiter = PinnedLink::from_stable(WaitLinkSource(NonNull::from(self.get_ref())));
        queue.get().unlink(waiter).is_some()
    }

    pub fn is_registered(&self) -> bool {
        self.queue.get().is_some()
    }
}

impl Default for Waiter<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        let Some(queue) = self.queue.get() else {
            return;
        };
        let waiter = PinnedLink::from_stable(WaitLinkSource(NonNull::from(&*self)));
        let _ = queue.get().unlink(waiter);
    }
}
