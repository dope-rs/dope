use core::array::from_fn;
use core::pin::{Pin, pin};
use core::task::Poll;
use std::process::abort;

use o3::collections::{ArrayVec, ArrayVecIntoIter};
use pin_project::{pin_project, pinned_drop};

use crate::abi::Fiber;
use crate::raw::pinned_slice;
use crate::raw::task::queue::IndexQueue;
use crate::raw::task::{Context, StableTaskSource, TaskContext};

struct BatchTask<'a, 'd> {
    context: Pin<&'a TaskContext<'d>>,
}

// SAFETY: the pinned BatchCore owns both endpoints. Its pinned Drop unbinds
// every live task before the task array, queue, or parent brand can disappear.
unsafe impl<'a, 'd> StableTaskSource<'a, 'd, usize> for BatchTask<'a, 'd> {
    fn context(self) -> Pin<&'a TaskContext<'d>> {
        self.context
    }
}

#[pin_project(project = SlotProj, project_replace = SlotProjOwn)]
enum Slot<F, O> {
    Vacant,
    Live(#[pin] F),
    Done(O),
}

impl<F, O> Slot<F, O> {
    const fn new() -> Self {
        Self::Vacant
    }

    fn write(&mut self, fiber: F) {
        debug_assert!(matches!(self, Self::Vacant));
        *self = Self::Live(fiber);
    }

    fn poll<'d>(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<O>
    where
        F: Fiber<'d, Output = O>,
    {
        match self.project() {
            SlotProj::Live(fiber) => Fiber::poll(fiber, context),
            SlotProj::Vacant | SlotProj::Done(_) => abort(),
        }
    }

    fn complete(self: Pin<&mut Self>, output: O) {
        match self.project_replace(Self::Done(output)) {
            SlotProjOwn::Live(_) => {}
            SlotProjOwn::Vacant | SlotProjOwn::Done(_) => abort(),
        }
    }

    fn take_output(self: Pin<&mut Self>) -> O {
        match self.project_replace(Self::Vacant) {
            SlotProjOwn::Done(output) => output,
            SlotProjOwn::Vacant | SlotProjOwn::Live(_) => abort(),
        }
    }

    fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }
}

pub(crate) enum PollStep {
    Idle,
    Pending,
    Ready,
}

#[pin_project(PinnedDrop, !Unpin)]
pub(crate) struct BatchCore<'d, F, O, const N: usize> {
    #[pin]
    slots: [Slot<F, O>; N],
    #[pin]
    tasks: [TaskContext<'d>; N],
    #[pin]
    ready: IndexQueue,
    len: usize,
    next_bind: usize,
}

impl<'d, F, O, const N: usize> BatchCore<'d, F, O, N> {
    pub(crate) fn new() -> Self {
        Self {
            slots: from_fn(|_| Slot::new()),
            tasks: from_fn(|_| TaskContext::new()),
            ready: IndexQueue::with_capacity(N),
            len: 0,
            next_bind: 0,
        }
    }

    pub(crate) fn from_array(fibers: [F; N]) -> Self {
        let mut core = Self::new();
        for (slot, fiber) in core.slots.iter_mut().zip(fibers) {
            slot.write(fiber);
        }
        core.len = N;
        core
    }

    pub(crate) fn try_push(&mut self, fiber: F) -> Result<(), F> {
        if self.next_bind != 0 || self.len == N {
            return Err(fiber);
        }
        self.slots[self.len].write(fiber);
        self.len += 1;
        Ok(())
    }

    pub(crate) fn poll_one(self: Pin<&mut Self>, mut context: Pin<&mut Context<'_, 'd>>) -> PollStep
    where
        F: Fiber<'d, Output = O>,
    {
        let mut this = self.project();
        let (index, wake) = if let Some(index) = this.ready.as_ref().pop() {
            let task =
                pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
            (index, TaskContext::waker(BatchTask { context: task }))
        } else if *this.next_bind < *this.len {
            let index = *this.next_bind;
            *this.next_bind += 1;
            let parent = context.as_ref().get_ref().parent_waker();
            let task =
                pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
            let wake = TaskContext::bind(
                BatchTask { context: task },
                this.ready.as_ref(),
                index,
                index,
                parent,
            );
            (index, wake)
        } else {
            return PollStep::Idle;
        };

        let mut child = pin!(Context::from_waker(wake, context.as_mut().driver_access()));
        let mut slot =
            pinned_slice::get_mut(this.slots.as_mut(), index).unwrap_or_else(|| unreachable!());
        let Poll::Ready(output) = slot.as_mut().poll(child.as_mut()) else {
            return PollStep::Pending;
        };
        let task = pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
        task.unbind();
        slot.complete(output);
        PollStep::Ready
    }

    pub(crate) fn has_work(self: Pin<&Self>) -> bool {
        let this = self.get_ref();
        this.next_bind < this.len || !this.ready.is_empty()
    }

    pub(crate) fn take_output(self: Pin<&mut Self>) -> BatchOutput<O, N> {
        let mut this = self.project();
        let len = *this.len;
        ArrayVec::from_fn(len, |index| {
            let slot =
                pinned_slice::get_mut(this.slots.as_mut(), index).unwrap_or_else(|| unreachable!());
            slot.take_output()
        })
        .into_iter()
    }
}

#[pinned_drop]
impl<'d, F, O, const N: usize> PinnedDrop for BatchCore<'d, F, O, N> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        for index in 0..*this.next_bind {
            let slot =
                pinned_slice::get(this.slots.as_ref(), index).unwrap_or_else(|| unreachable!());
            let task =
                pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
            if slot.is_live() && task.is_bound() {
                task.unbind();
            }
        }
    }
}

pub type BatchOutput<O, const N: usize> = ArrayVecIntoIter<O, N>;
