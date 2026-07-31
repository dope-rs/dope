use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::process::abort;
use std::task::Poll;

use dope::DriverContext;

use crate::raw::pinned_slice;
use crate::raw::task::queue::TaskQueue;
use crate::raw::task::{RootWaker, StableTaskSource, TaskContext};
use crate::slab::{Slab, TaskId};
use crate::{Context, Fiber};

struct SlabTask<'a, 'd, T: Copy> {
    context: Pin<&'a TaskContext<T>>,
    _brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

// SAFETY: TaskSlab owns every pinned context and unbinds it after its fiber is
// removed; queue Drop detaches the reverse link before the queue disappears.
unsafe impl<'a, 'd, T: Copy> StableTaskSource<'a, 'd, T> for SlabTask<'a, 'd, T> {
    fn context(self) -> Pin<&'a TaskContext<T>> {
        self.context
    }
}

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
        pinned_slice::get(self.contexts.as_ref(), index)
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
        let _ = TaskContext::bind(
            SlabTask {
                context,
                _brand: PhantomData,
            },
            queue,
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
        let wake = TaskContext::waker(SlabTask {
            context,
            _brand: PhantomData,
        });
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
        context.unbind();
        true
    }
}
