use core::marker::PhantomPinned;
use core::pin::Pin;
use core::ptr::NonNull;

use o3::cell::RawCell;
use o3::collections::{BatchDrain, BatchSet};

use super::{BindingQueue, BindingSource, RootWaker, TaskContext, Waker};
use crate::raw::link::PinnedLink;

struct Slot<T: Copy> {
    target: T,
    task: Option<PinnedLink<TaskContext<T>>>,
}

struct TaskSlots<T: Copy> {
    values: Vec<Slot<T>>,
    free: Vec<usize>,
}

pub struct TaskQueue<T: Copy = usize> {
    pub(super) ready: BatchSet,
    slots: RawCell<TaskSlots<T>>,
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
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ready: BatchSet::with_capacity(capacity),
            slots: RawCell::new(TaskSlots {
                values: Vec::with_capacity(capacity),
                free: Vec::with_capacity(capacity),
            }),
            _pin: PhantomPinned,
        }
    }

    #[inline(always)]
    fn update_slots<R>(&self, update: impl FnOnce(&mut TaskSlots<T>) -> R) -> R {
        // SAFETY: `TaskQueue` owns the storage and exposes mutation only
        // through `allocate` and `recycle`. Neither operation calls user code
        // or reenters this queue, and shared slot access returns only `T: Copy`.
        unsafe { self.slots.with_mut(update) }
    }

    pub(super) fn recycle(&self, index: usize) {
        self.update_slots(|slots| {
            let slot = &mut slots.values[index];
            debug_assert!(slot.task.is_some());
            slot.task = None;
            slots.free.push(index);
        });
    }

    fn target(&self, index: usize) -> T {
        self.slots.with(|slots| slots.values[index].target)
    }

    pub fn is_empty(self: Pin<&Self>) -> bool {
        self.ready.is_empty()
    }

    pub fn snapshot_root<'queue, 'root>(
        self: Pin<&'queue Self>,
        parent: RootWaker<'root>,
    ) -> Option<impl Iterator<Item = T> + use<'queue, 'root, T>> {
        self.snapshot_inner(parent.into())
    }

    fn snapshot_inner<'queue, 'parent>(
        self: Pin<&'queue Self>,
        parent: Waker<'parent>,
    ) -> Option<TaskSnapshot<'queue, 'parent, T>> {
        let queue = self.get_ref();
        let ready: &'queue BatchSet = &queue.ready;
        Some(TaskSnapshot {
            queue,
            drain: Some(ready.drain_batch()?),
            parent,
            exhausted: false,
        })
    }
}

impl<T: Copy> Drop for TaskQueue<T> {
    fn drop(&mut self) {
        let queue = PinnedLink::from_stable(BindingSource(NonNull::from(&*self)));
        for (index, slot) in self.slots.get_mut().values.iter_mut().enumerate() {
            let Some(task) = slot.task.take() else {
                continue;
            };
            task.get().detach_queue(queue, index);
        }
    }
}

// SAFETY: occupied slots retain tasks until recycle, and Drop detaches every
// remaining task before this queue and its ready set disappear.
unsafe impl<T: Copy> BindingQueue<T> for TaskQueue<T> {
    type Input = T;

    fn attach(self: Pin<&Self>, target: Self::Input, task: PinnedLink<TaskContext<T>>) -> usize {
        self.update_slots(|slots| {
            if let Some(index) = slots.free.pop() {
                let slot = &mut slots.values[index];
                debug_assert!(slot.task.is_none());
                slot.target = target;
                slot.task = Some(task);
                index
            } else {
                let index = slots.values.len();
                self.ready.grow_to(index + 1);
                slots.values.push(Slot {
                    target,
                    task: Some(task),
                });
                index
            }
        })
    }

    fn ready(&self) -> &BatchSet {
        &self.ready
    }

    fn recycle_link(self: Pin<&Self>) -> Option<PinnedLink<TaskQueue<T>>> {
        Some(PinnedLink::from_stable(BindingSource(NonNull::from(
            self.get_ref(),
        ))))
    }
}

// SAFETY: IndexQueue retains no task link. BatchCore owns the ready set and
// tasks together and unbinds them in pinned Drop.
unsafe impl BindingQueue<usize> for IndexQueue {
    type Input = usize;

    fn attach(
        self: Pin<&Self>,
        index: Self::Input,
        _task: PinnedLink<TaskContext<usize>>,
    ) -> usize {
        debug_assert!(index < self.ready.capacity());
        index
    }

    fn ready(&self) -> &BatchSet {
        &self.ready
    }
}

struct TaskSnapshot<'queue, 'parent, T: Copy> {
    queue: &'queue TaskQueue<T>,
    drain: Option<BatchDrain<'queue>>,
    parent: Waker<'parent>,
    exhausted: bool,
}

impl<T: Copy> Iterator for TaskSnapshot<'_, '_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.drain.as_mut()?.next();
        self.exhausted = next.is_none();
        next.map(|index| self.queue.target(index))
    }
}

impl<T: Copy> Drop for TaskSnapshot<'_, '_, T> {
    fn drop(&mut self) {
        self.drain.take();
        if !self.exhausted {
            self.parent.wake();
        }
    }
}
