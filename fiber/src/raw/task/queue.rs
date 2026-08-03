use core::marker::PhantomPinned;
use core::pin::Pin;
use core::ptr::NonNull;

use o3::collections::__private::{BatchMap, BatchMapDrain};
use o3::collections::BatchSet;

use super::{BindingQueue, BindingSource, RootWaker, Waker};
use crate::raw::link::PinnedLink;

pub(crate) struct Queue<T: Copy = usize> {
    ready: BatchMap<T>,
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

impl<T: Copy> Queue<T> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            ready: BatchMap::with_capacity(capacity),
            _pin: PhantomPinned,
        }
    }

    pub(super) fn clear(&self, index: usize) {
        // SAFETY: TaskContext clears only after Node removes this ready index.
        unsafe { self.ready.clear_unchecked(index) }
    }

    pub(crate) fn is_empty(self: Pin<&Self>) -> bool {
        self.ready.is_empty()
    }

    pub(crate) fn snapshot_root<'queue, 'root>(
        self: Pin<&'queue Self>,
        parent: RootWaker<'root>,
    ) -> Option<impl Iterator<Item = T> + use<'queue, 'root, T>> {
        self.snapshot_inner(parent.into())
    }

    fn snapshot_inner<'queue, 'parent>(
        self: Pin<&'queue Self>,
        parent: Waker<'parent>,
    ) -> Option<Snapshot<'queue, 'parent, T>> {
        let queue = self.get_ref();
        Some(Snapshot {
            drain: Some(queue.ready.drain_batch()?),
            parent,
            exhausted: false,
        })
    }
}

// SAFETY: TaskSlab validates indices; binding initializes before exposure, and
// teardown removes ready before clearing a target or revoking its node link.
unsafe impl<T: Copy> BindingQueue<T> for Queue<T> {
    type Input = T;

    fn attach(self: Pin<&Self>, index: usize, target: Self::Input) -> usize {
        // SAFETY: TaskSlab supplies a live vacant slot within queue capacity.
        unsafe { self.ready.bind_unchecked(index, target) }
        index
    }

    fn ready(&self) -> &BatchSet {
        // SAFETY: attach binds before this link is exposed; unbind removes
        // ready before clear, and bound node links cannot wake after unbind.
        unsafe { self.ready.ready_set() }
    }

    fn recycle_link(self: Pin<&Self>) -> Option<PinnedLink<Queue<T>>> {
        Some(PinnedLink::from_stable(BindingSource(NonNull::from(
            self.get_ref(),
        ))))
    }
}

// SAFETY: IndexQueue retains no task link. BatchCore owns the ready set and
// tasks together and unbinds them in pinned Drop.
unsafe impl BindingQueue<usize> for IndexQueue {
    type Input = usize;

    fn attach(self: Pin<&Self>, index: usize, _input: Self::Input) -> usize {
        debug_assert!(index < self.ready.capacity());
        index
    }

    fn ready(&self) -> &BatchSet {
        &self.ready
    }
}

struct Snapshot<'queue, 'parent, T: Copy> {
    drain: Option<BatchMapDrain<'queue, T>>,
    parent: Waker<'parent>,
    exhausted: bool,
}

impl<T: Copy> Iterator for Snapshot<'_, '_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.drain.as_mut()?.next();
        self.exhausted = next.is_none();
        next
    }
}

impl<T: Copy> Drop for Snapshot<'_, '_, T> {
    fn drop(&mut self) {
        self.drain.take();
        if !self.exhausted {
            self.parent.wake();
        }
    }
}
