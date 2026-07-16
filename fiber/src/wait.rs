use std::cell::Cell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

use o3::cell::RawCell;
use o3::collections::CellBitmap;

use crate::task::{Context, Waker};

pub struct WaitQueue<T = ()> {
    occupied: CellBitmap,
    slots: RawCell<Vec<Option<NonNull<Waiter<'static, T>>>>>,
    free: RawCell<Vec<usize>>,
    len: Cell<usize>,
    capacity: usize,
    _pin: PhantomPinned,
}

pub struct Waiter<'d, T = ()> {
    queue: Cell<Option<NonNull<WaitQueue<T>>>>,
    index: Cell<usize>,
    wake: Cell<Option<Waker<'d>>>,
    assigned: Cell<Option<T>>,
    _pin: PhantomPinned,
}

impl WaitQueue<()> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::build(capacity)
    }
}

impl<T> WaitQueue<T> {
    fn build(capacity: usize) -> Self {
        Self {
            occupied: CellBitmap::with_capacity(capacity),
            slots: RawCell::new(Vec::with_capacity(capacity)),
            free: RawCell::new(Vec::with_capacity(capacity)),
            len: Cell::new(0),
            capacity,
            _pin: PhantomPinned,
        }
    }

    pub fn with_payload_capacity(capacity: usize) -> Self {
        Self::build(capacity)
    }

    pub fn can_register<'d>(self: Pin<&Self>, waiter: Pin<&Waiter<'d, T>>) -> bool {
        waiter.queue.get() == Some(NonNull::from(self.get_ref())) || self.len.get() < self.capacity
    }

    #[must_use]
    pub fn try_register<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d, T>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.try_register_waker(waiter, unsafe { context.waker_unchecked() })
    }

    #[doc(hidden)]
    pub fn try_register_waker<'d>(
        self: Pin<&Self>,
        waiter: Pin<&Waiter<'d, T>>,
        waker: Waker<'d>,
    ) -> bool {
        let queue = NonNull::from(self.get_ref());
        if waiter.queue.get() == Some(queue) {
            waiter.wake.set(Some(waker));
            return true;
        }
        if self.len.get() == self.capacity {
            return false;
        }
        waiter.unregister();
        let index = unsafe {
            self.free.with_mut(|free| {
                self.slots.with_mut(|slots| {
                    let index = free.pop().unwrap_or_else(|| {
                        let index = slots.len();
                        slots.push(None);
                        index
                    });
                    slots[index] = Some(NonNull::from(waiter.get_ref()).cast());
                    index
                })
            })
        };
        self.occupied.insert(index);
        self.len.set(self.len.get() + 1);
        waiter.queue.set(Some(queue));
        waiter.index.set(index);
        waiter.wake.set(Some(waker));
        true
    }

    fn unlink<'d>(self: Pin<&Self>, waiter: NonNull<Waiter<'d, T>>) -> Option<Waker<'d>> {
        let waiter_ref = unsafe { waiter.as_ref() };
        if waiter_ref.queue.get() != Some(NonNull::from(self.get_ref())) {
            return None;
        }
        let index = waiter_ref.index.replace(usize::MAX);
        self.occupied.remove(index);
        unsafe { self.slots.with_mut(|slots| slots[index] = None) };
        unsafe { self.free.with_mut(|free| free.push(index)) };
        self.len.set(self.len.get() - 1);
        waiter_ref.queue.set(None);
        waiter_ref.wake.take()
    }

    fn pop_next(self: Pin<&Self>, assigned: Option<T>, wake: bool) -> Result<(), Option<T>> {
        let Some(index) = self.occupied.pop_next() else {
            return Err(assigned);
        };
        let waiter = unsafe {
            self.slots
                .with_mut(|slots| slots[index].take().unwrap().cast::<Waiter<'_, T>>())
        };
        unsafe { self.free.with_mut(|free| free.push(index)) };
        self.len.set(self.len.get() - 1);
        let waiter = unsafe { waiter.as_ref() };
        waiter.queue.set(None);
        waiter.index.set(usize::MAX);
        waiter.assigned.set(assigned);
        if let Some(waker) = waiter.wake.take()
            && wake
        {
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
        self.len.get() == 0
    }
}

impl<T> Drop for WaitQueue<T> {
    fn drop(&mut self) {
        let queue = unsafe { Pin::new_unchecked(&*self) };
        while queue.pop_next(None, false).is_ok() {}
    }
}

impl<'d, T> Waiter<'d, T> {
    pub const fn new() -> Self {
        Self {
            queue: Cell::new(None),
            index: Cell::new(usize::MAX),
            wake: Cell::new(None),
            assigned: Cell::new(None),
            _pin: PhantomPinned,
        }
    }

    pub fn unregister(self: Pin<&Self>) -> bool {
        let Some(queue) = self.queue.get() else {
            return false;
        };
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
        unsafe { Pin::new_unchecked(queue.as_ref()) }.unlink(NonNull::from(&*self));
    }
}
