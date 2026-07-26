use std::pin::{Pin, pin};
use std::process::abort;
use std::task::Poll;

use dope::DriverContext;

use crate::raw::task::queue::TaskQueue;
use crate::raw::task::{RootWaker, TaskContext};
use crate::slab::{Slab, TaskId};
use crate::{Context, Fiber};

/// A fiber slab whose persistent wake nodes share each fiber's lifetime.
///
/// Removal drops the fiber before its wake node; queue drop detaches every node.
pub struct TaskSlab<'d, F, T: Copy = usize, Tag = ()>
where
    F: Fiber<'d>,
{
    fibers: Slab<'d, F, Tag>,
    contexts: Pin<Box<[TaskContext<T>]>>,
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
        }
    }

    fn context(&self, index: usize) -> Option<Pin<&TaskContext<T>>> {
        let context = self.contexts.as_ref().get_ref().get(index)?;
        // SAFETY: pinning the boxed slice pins every context for the lifetime
        // of this slab.
        Some(unsafe { Pin::new_unchecked(context) })
    }

    pub fn insert(&mut self, fiber: F) -> Option<TaskId<Tag>> {
        self.fibers.insert(fiber)
    }

    pub fn bind(
        &self,
        id: &TaskId<Tag>,
        queue: Pin<&TaskQueue<T>>,
        target: T,
        parent: RootWaker<'d>,
    ) -> bool {
        if !self.fibers.contains(id) {
            return false;
        }
        let Some(context) = self.context(id.index()) else {
            return false;
        };
        if context.is_bound() {
            return false;
        }
        // SAFETY: this slab owns the pinned context and corresponding fiber as
        // one entry. Queue drop detaches the context, while removal drops the
        // fiber before unbinding the context.
        let _ = unsafe { context.bind_inner(queue, target, Some(parent.into())) };
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
        // SAFETY: the corresponding fiber and persistent binding are live for
        // this slab's driver brand `'d`.
        let wake = unsafe { context.context_unchecked() };
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

    pub fn remove(&mut self, id: TaskId<Tag>) -> bool {
        let index = id.index();
        if !self.fibers.remove(id) {
            return false;
        }
        let Some(context) = self.context(index) else {
            abort();
        };
        // SAFETY: the fiber has just been dropped, so none of its asynchronous
        // registrations can retain this wake node.
        unsafe { context.unbind() };
        true
    }
}
