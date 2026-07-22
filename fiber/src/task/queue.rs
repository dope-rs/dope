use core::cell::Cell;
use core::marker::PhantomPinned;
use core::pin::Pin;
use core::ptr::NonNull;

use o3::cell::RawCell;
use o3::collections::{BatchDrain, BatchSet};

use super::{RootWaker, TaskContext, Waker};

struct Slot<T: Copy> {
    target: Cell<T>,
    task: Cell<Option<NonNull<TaskContext<T>>>>,
}

pub struct TaskQueue<T: Copy = usize> {
    pub(super) ready: BatchSet,
    values: RawCell<Vec<Slot<T>>>,
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

    pub(super) fn allocate(&self, target: T, task: NonNull<TaskContext<T>>) -> usize {
        unsafe {
            self.free.with_mut(|free| {
                self.values.with_mut(|values| {
                    if let Some(index) = free.pop() {
                        values[index].target.set(target);
                        values[index].task.set(Some(task));
                        index
                    } else {
                        let index = values.len();
                        values.push(Slot {
                            target: Cell::new(target),
                            task: Cell::new(Some(task)),
                        });
                        self.ready.grow_to(index + 1);
                        index
                    }
                })
            })
        }
    }

    pub(super) fn release(&self, index: usize) {
        self.ready.remove(index);
        unsafe { self.values.with(|values| values[index].task.set(None)) };
        unsafe { self.free.with_mut(|free| free.push(index)) };
    }

    pub(super) fn set_target(&self, index: usize, target: T) {
        unsafe { self.values.with(|values| values[index].target.set(target)) };
    }

    fn target(&self, index: usize) -> T {
        unsafe { self.values.with(|values| values[index].target.get()) }
    }

    pub fn pop(self: Pin<&Self>) -> Option<T> {
        self.ready.pop().map(|index| self.target(index))
    }

    pub fn is_empty(self: Pin<&Self>) -> bool {
        self.ready.is_empty()
    }

    pub fn snapshot<'d>(
        self: Pin<&'d Self>,
        parent: Waker<'d>,
    ) -> Option<impl Iterator<Item = T> + use<'d, T>> {
        self.snapshot_inner(parent)
    }

    pub fn snapshot_root<'queue, 'root>(
        self: Pin<&'queue Self>,
        parent: RootWaker<'root>,
    ) -> Option<impl Iterator<Item = T> + use<'queue, T>>
    where
        'root: 'queue,
    {
        self.snapshot_inner(parent.into_waker().shorten())
    }

    fn snapshot_inner<'d>(self: Pin<&'d Self>, parent: Waker<'d>) -> Option<TaskSnapshot<'d, T>> {
        let queue = self.get_ref();
        let ready: &'d BatchSet = &queue.ready;
        Some(TaskSnapshot {
            queue,
            drain: Some(ready.drain_batch()?),
            parent,
            exhausted: false,
        })
    }
}

impl<T: Copy> Default for TaskQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy> Drop for TaskQueue<T> {
    fn drop(&mut self) {
        let queue = NonNull::from(&*self);
        // SAFETY: every occupied slot was installed from a pinned task. Queue
        // Drop runs while its ready set is still live and detaches each node
        // before the queue storage can disappear.
        unsafe {
            self.values.with(|values| {
                for (index, slot) in values.iter().enumerate() {
                    let Some(task) = slot.task.take() else {
                        continue;
                    };
                    Pin::new_unchecked(task.as_ref()).detach_queue(queue, index);
                }
            });
        }
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
