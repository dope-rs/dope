use core::cell::Cell;
use core::marker::PhantomPinned;
use core::pin::Pin;

use o3::cell::RawCell;
use o3::collections::{BatchDrain, BatchSet};

use super::Waker;

pub struct TaskQueue<T: Copy = usize> {
    pub(super) ready: BatchSet,
    values: RawCell<Vec<Cell<T>>>,
    free: RawCell<Vec<usize>>,
    _pin: PhantomPinned,
}

pub(crate) struct IndexQueue {
    pub(super) ready: BatchSet,
    _pin: PhantomPinned,
}

impl IndexQueue {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            ready: BatchSet::with_capacity(capacity),
            _pin: PhantomPinned,
        }
    }

    pub(crate) fn pop(self: Pin<&Self>) -> Option<usize> {
        self.ready.pop()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }
}

impl<T: Copy> TaskQueue<T> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ready: BatchSet::with_capacity(capacity),
            values: RawCell::new(Vec::with_capacity(capacity)),
            free: RawCell::new(Vec::with_capacity(capacity)),
            _pin: PhantomPinned,
        }
    }

    pub(super) fn allocate(&self, target: T) -> usize {
        unsafe {
            self.free.with_mut(|free| {
                self.values.with_mut(|values| {
                    if let Some(index) = free.pop() {
                        values[index].set(target);
                        index
                    } else {
                        let index = values.len();
                        values.push(Cell::new(target));
                        self.ready.grow_to(index + 1);
                        index
                    }
                })
            })
        }
    }

    pub(super) fn release(&self, index: usize) {
        self.ready.remove(index);
        unsafe { self.free.with_mut(|free| free.push(index)) };
    }

    pub(super) fn set_target(&self, index: usize, target: T) {
        unsafe { self.values.with(|values| values[index].set(target)) };
    }

    fn target(&self, index: usize) -> T {
        unsafe { self.values.with(|values| values[index].get()) }
    }

    pub fn pop(self: Pin<&Self>) -> Option<T> {
        self.ready.pop().map(|index| self.target(index))
    }

    pub fn is_empty(self: Pin<&Self>) -> bool {
        self.ready.is_empty()
    }

    /// # Safety
    /// No other snapshot may be active for this queue.
    pub unsafe fn snapshot<'d>(
        self: Pin<&'d Self>,
        parent: Waker<'d>,
    ) -> impl Iterator<Item = T> + use<'d, T> {
        let queue = self.get_ref();
        let ready: &'d BatchSet = &queue.ready;
        TaskSnapshot {
            queue,
            drain: ready.drain_batch(),
            parent,
            exhausted: false,
        }
    }
}

impl<T: Copy> Default for TaskQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct TaskSnapshot<'d, T: Copy> {
    queue: &'d TaskQueue<T>,
    drain: Option<BatchDrain<'d>>,
    parent: Waker<'d>,
    exhausted: bool,
}

impl<T: Copy> Iterator for TaskSnapshot<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.drain.as_mut()?.next();
        self.exhausted = next.is_none();
        next.map(|index| self.queue.target(index))
    }
}

impl<T: Copy> Drop for TaskSnapshot<'_, T> {
    fn drop(&mut self) {
        self.drain.take();
        if !self.exhausted {
            self.parent.wake();
        }
    }
}
