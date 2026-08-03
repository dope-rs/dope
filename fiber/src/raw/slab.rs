use std::pin::{Pin, pin};
use std::process::abort;
use std::task::Poll;

use dope::DriverContext;

use crate::raw::pinned_slice;
use crate::raw::task::queue::Queue;
use crate::raw::task::{RootWaker, StableTaskSource, TaskContext};
use crate::slab::{Slab, TaskId};
use crate::{Context, Fiber};

struct SlabTask<'a, 'd, T: Copy> {
    context: Pin<&'a TaskContext<'d, T>>,
}

// SAFETY: TaskSlab owns its pinned contexts and queue; context teardown runs
// before the queue is dropped.
unsafe impl<'a, 'd, T: Copy> StableTaskSource<'a, 'd, T> for SlabTask<'a, 'd, T> {
    fn context(self) -> Pin<&'a TaskContext<'d, T>> {
        self.context
    }
}

/// A fiber slab whose persistent wake nodes and ready queue share one owner.
pub struct TaskSlab<'d, F, T: Copy = usize, Tag = ()>
where
    F: Fiber<'d>,
{
    fibers: Slab<'d, F, Tag>,
    contexts: Pin<Box<[TaskContext<'d, T>]>>,
    queue: Pin<Box<Queue<T>>>,
}

impl<'d, F, T, Tag> TaskSlab<'d, F, T, Tag>
where
    F: Fiber<'d>,
    T: Copy,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fibers: Slab::with_capacity(capacity),
            contexts: Box::into_pin(
                (0..capacity)
                    .map(|_| TaskContext::new())
                    .collect::<Box<[_]>>(),
            ),
            queue: Box::pin(Queue::with_capacity(capacity)),
        }
    }

    fn context(&self, index: usize) -> Option<Pin<&TaskContext<'d, T>>> {
        pinned_slice::get(self.contexts.as_ref(), index)
    }

    pub fn insert(&mut self, fiber: F) -> Option<TaskId<Tag>> {
        self.fibers.insert(fiber)
    }

    pub fn bind(&self, id: &TaskId<Tag>, target: T, parent: RootWaker<'d>) -> bool {
        if !self.fibers.contains(id) {
            return false;
        }
        let Some(context) = self.context(id.index()) else {
            return false;
        };
        if context.is_bound() {
            return false;
        }
        let _ = TaskContext::bind(
            SlabTask { context },
            self.queue.as_ref(),
            id.index(),
            target,
            parent.into(),
        );
        true
    }

    pub fn poll(
        &mut self,
        id: &TaskId<Tag>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Option<Poll<F::Output>> {
        let index = id.index();
        if !self.fibers.contains(id) {
            return None;
        }
        let context = self.context(index)?;
        if !context.is_bound() {
            return None;
        }
        let wake = TaskContext::waker(SlabTask { context });
        let mut context = pin!(Context::from_waker(wake, driver.reborrow()));
        self.fibers.poll(id, context.as_mut())
    }

    pub fn wake(&self, id: &TaskId<Tag>) -> bool {
        if !self.fibers.contains(id) {
            return false;
        }
        let Some(context) = self.context(id.index()) else {
            return false;
        };
        if !context.is_bound() {
            return false;
        }
        context.wake();
        true
    }

    pub fn is_empty(&self) -> bool {
        self.queue.as_ref().is_empty()
    }

    pub fn snapshot_root<'slab, 'root>(
        &'slab self,
        parent: RootWaker<'root>,
    ) -> Option<impl Iterator<Item = T> + use<'slab, 'root, 'd, F, T, Tag>> {
        self.queue.as_ref().snapshot_root(parent)
    }

    pub fn remove(&mut self, id: TaskId<Tag>) -> bool {
        let index = id.index();
        if !self.fibers.remove(id) {
            return false;
        }
        let Some(context) = self.context(index) else {
            abort();
        };
        context.unbind();
        true
    }
}
