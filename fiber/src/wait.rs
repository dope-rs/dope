use std::cell::Cell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

use o3::marker::ThreadBound;

use crate::task::{Context, Waker};

type WaiterPtr<T> = NonNull<Waiter<'static, T>>;

/// A bounded, allocation-free FIFO of pinned waiters.
///
/// The queue and its waiters unlink each other during `Drop`, so either side
/// may be dropped first. All raw links stay inside this module; the public API
/// requires both endpoints to be pinned before a link can be created.
pub struct WaitQueue<T = ()> {
    head: Cell<Option<WaiterPtr<T>>>,
    tail: Cell<Option<WaiterPtr<T>>>,
    len: Cell<usize>,
    capacity: usize,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

pub struct Waiter<'d, T = ()> {
    queue: Cell<Option<NonNull<WaitQueue<T>>>>,
    previous: Cell<Option<WaiterPtr<T>>>,
    next: Cell<Option<WaiterPtr<T>>>,
    wake: Cell<Option<Waker<'d>>>,
    assigned: Cell<Option<T>>,
    _pin: PhantomPinned,
    _thread: ThreadBound,
}

impl WaitQueue<()> {
    pub const fn with_capacity(capacity: usize) -> Self {
        Self::build(capacity)
    }
}

impl<T> WaitQueue<T> {
    const fn build(capacity: usize) -> Self {
        Self {
            head: Cell::new(None),
            tail: Cell::new(None),
            len: Cell::new(0),
            capacity,
            _pin: PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }

    pub const fn with_payload_capacity(capacity: usize) -> Self {
        Self::build(capacity)
    }

    /// Projects one queue from a pinned slice without exposing the intrusive
    /// queue's pinning proof to consumers.
    pub fn get_pinned(queues: Pin<&[Self]>, index: usize) -> Option<Pin<&Self>> {
        let queue = queues.get_ref().get(index)?;
        // SAFETY: pinning a slice pins every element, and the shared slice
        // reference cannot move or replace an element.
        Some(unsafe { Pin::new_unchecked(queue) })
    }

    fn contains<'d>(self: Pin<&Self>, waiter: Pin<&Waiter<'d, T>>) -> bool {
        waiter.queue.get() == Some(NonNull::from(self.get_ref()))
    }

    pub fn can_register<'d>(self: Pin<&Self>, waiter: Pin<&Waiter<'d, T>>) -> bool {
        self.contains(waiter) || self.len.get() < self.capacity
    }

    #[must_use]
    pub fn try_register<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d, T>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        // SAFETY: `waiter` carries the same driver brand `'d` and owns the
        // copied waker until it is unlinked or dropped.
        self.try_register_waker(waiter, unsafe { context.waker_unchecked() })
    }

    #[doc(hidden)]
    pub fn try_register_waker<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d, T>>,
        waker: Waker<'d>,
    ) -> bool {
        if self.contains(waiter) {
            waiter.wake.set(Some(waker));
            return true;
        }
        if self.len.get() == self.capacity {
            return false;
        }

        // A waiter may move between queues. A failed registration above leaves
        // its old registration intact; a successful one first detaches it.
        waiter.unregister();
        debug_assert!(waiter.previous.get().is_none());
        debug_assert!(waiter.next.get().is_none());

        let queue = NonNull::from(self.get_ref());
        let node = NonNull::from(waiter.get_ref()).cast::<Waiter<'static, T>>();
        let previous = self.tail.get();
        waiter.queue.set(Some(queue));
        waiter.previous.set(previous);
        waiter.wake.set(Some(waker));
        if let Some(previous) = previous {
            // SAFETY: every pointer in this queue names a live pinned waiter.
            // We only update its interior link cell on this single thread.
            unsafe { previous.as_ref() }.next.set(Some(node));
        } else {
            self.head.set(Some(node));
        }
        self.tail.set(Some(node));
        self.len.set(self.len.get() + 1);
        true
    }

    fn unlink<'d>(self: Pin<&Self>, waiter: NonNull<Waiter<'d, T>>) -> Option<Waker<'d>> {
        // SAFETY: callers obtain this pointer either from a pinned `Waiter` or
        // from this queue's links. Both Drop implementations unlink before an
        // endpoint's storage can cease to be live.
        let waiter = unsafe { waiter.as_ref() };
        if waiter.queue.get() != Some(NonNull::from(self.get_ref())) {
            return None;
        }

        let previous = waiter.previous.take();
        let next = waiter.next.take();
        if let Some(previous) = previous {
            // SAFETY: linked neighbours are live and pinned by the invariant
            // stated on `WaitQueue`.
            unsafe { previous.as_ref() }.next.set(next);
        } else {
            self.head.set(next);
        }
        if let Some(next) = next {
            // SAFETY: same invariant as for `previous` above.
            unsafe { next.as_ref() }.previous.set(previous);
        } else {
            self.tail.set(previous);
        }
        waiter.queue.set(None);
        self.len.set(self.len.get() - 1);
        waiter.wake.take()
    }

    fn pop_next(self: Pin<&Self>, assigned: Option<T>, wake: bool) -> Result<(), Option<T>> {
        let Some(node) = self.head.get() else {
            return Err(assigned);
        };
        let waiter = node.cast::<Waiter<'_, T>>();
        let waker = self
            .unlink(waiter)
            .expect("dope-fiber: linked waiter missing its queue");
        // SAFETY: unlinking removes the raw links but does not end the pinned
        // waiter's lifetime. The owner cannot drop it concurrently.
        unsafe { waiter.as_ref() }.assigned.set(assigned);
        if wake {
            waker.wake();
        }
        Ok(())
    }

    pub fn wake(self: Pin<&Self>) {
        while self.pop_next(None, true).is_ok() {}
    }

    pub fn wake_one(self: Pin<&Self>) {
        let _ = self.pop_next(None, true);
    }

    pub fn assign_one(self: Pin<&Self>, value: T) -> Result<(), T> {
        match self.pop_next(Some(value), true) {
            Err(Some(value)) => Err(value),
            _ => Ok(()),
        }
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Drop for WaitQueue<T> {
    fn drop(&mut self) {
        // SAFETY: a linked queue can only have been accessed through a `Pin`.
        // Its address remains stable through Drop, and `pop_next` clears every
        // waiter's back-link before the queue storage goes away.
        let queue = unsafe { Pin::new_unchecked(&*self) };
        while queue.pop_next(None, false).is_ok() {}
    }
}

impl<'d, T> Waiter<'d, T> {
    pub const fn new() -> Self {
        Self {
            queue: Cell::new(None),
            previous: Cell::new(None),
            next: Cell::new(None),
            wake: Cell::new(None),
            assigned: Cell::new(None),
            _pin: PhantomPinned,
            _thread: ThreadBound::NEW,
        }
    }

    pub fn unregister(self: Pin<&Self>) -> bool {
        let Some(queue) = self.queue.get() else {
            return false;
        };
        // SAFETY: a live registration guarantees the queue is still live and
        // pinned. Queue::drop clears every registration before returning.
        unsafe { Pin::new_unchecked(queue.as_ref()) }
            .unlink(NonNull::from(self.get_ref()))
            .is_some()
    }

    pub fn is_registered(&self) -> bool {
        self.queue.get().is_some()
    }

    pub fn take_assigned(&self) -> Option<T> {
        self.assigned.take()
    }
}

impl<T> Default for Waiter<'_, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Waiter<'_, T> {
    fn drop(&mut self) {
        let Some(queue) = self.queue.get() else {
            return;
        };
        // SAFETY: Drop cannot move this waiter before it runs. A non-null
        // back-link means queue Drop has not detached it, so the pinned queue
        // and its links are still live.
        let queue = unsafe { Pin::new_unchecked(queue.as_ref()) };
        let _ = queue.unlink(NonNull::from(&*self));
    }
}
