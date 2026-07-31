use core::array::from_fn;
use core::hint;
use core::marker::PhantomData;
use core::mem::{MaybeUninit, forget};
use core::pin::{Pin, pin};
use core::task::Poll;

use pin_project::{pin_project, pinned_drop};

use crate::abi::Fiber;
use crate::raw::pinned_slice;
use crate::raw::task::queue::IndexQueue;
use crate::raw::task::{Context, StableTaskSource, TaskContext};

struct BatchTask<'a, 'd> {
    context: Pin<&'a TaskContext>,
    _brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

// SAFETY: the pinned BatchCore owns both endpoints. Its pinned Drop unbinds
// every live task before the task array, queue, or parent brand can disappear.
unsafe impl<'a, 'd> StableTaskSource<'a, 'd, usize> for BatchTask<'a, 'd> {
    fn context(self) -> Pin<&'a TaskContext> {
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
            // SAFETY: the scheduler only queues initialized, incomplete slots.
            SlotProj::Vacant | SlotProj::Done(_) => unsafe {
                debug_assert!(false, "dope: polled an inactive batch slot");
                hint::unreachable_unchecked()
            },
        }
    }

    fn complete(self: Pin<&mut Self>, output: O) {
        match self.project_replace(Self::Done(output)) {
            SlotProjOwn::Live(_) => {}
            // SAFETY: only a live slot can produce a ready fiber output.
            SlotProjOwn::Vacant | SlotProjOwn::Done(_) => unsafe {
                debug_assert!(false, "dope: completed an inactive batch slot");
                hint::unreachable_unchecked()
            },
        }
    }

    fn take_output(self: Pin<&mut Self>) -> O {
        match self.project_replace(Self::Vacant) {
            SlotProjOwn::Done(output) => output,
            // SAFETY: output extraction starts only after every slot completed.
            SlotProjOwn::Vacant | SlotProjOwn::Live(_) => unsafe {
                debug_assert!(false, "dope: extracted an incomplete batch slot");
                hint::unreachable_unchecked()
            },
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
pub(crate) struct BatchCore<F, O, const N: usize> {
    #[pin]
    slots: [Slot<F, O>; N],
    #[pin]
    tasks: [TaskContext; N],
    #[pin]
    ready: IndexQueue,
    len: usize,
    next_bind: usize,
}

impl<F, O, const N: usize> BatchCore<F, O, N> {
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

    pub(crate) fn poll_one<'d>(
        self: Pin<&mut Self>,
        mut context: Pin<&mut Context<'_, 'd>>,
    ) -> PollStep
    where
        F: Fiber<'d, Output = O>,
    {
        let mut this = self.project();
        let (index, wake) = if let Some(index) = this.ready.as_ref().pop() {
            let task =
                pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
            (
                index,
                TaskContext::waker(BatchTask {
                    context: task,
                    _brand: PhantomData,
                }),
            )
        } else if *this.next_bind < *this.len {
            let index = *this.next_bind;
            *this.next_bind += 1;
            let parent = context.as_ref().get_ref().parent_waker();
            let task =
                pinned_slice::get(this.tasks.as_ref(), index).unwrap_or_else(|| unreachable!());
            let wake = TaskContext::bind(
                BatchTask {
                    context: task,
                    _brand: PhantomData,
                },
                this.ready.as_ref(),
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
        let mut outputs = from_fn(|_| MaybeUninit::uninit());
        for (index, output) in outputs[..len].iter_mut().enumerate() {
            let slot =
                pinned_slice::get_mut(this.slots.as_mut(), index).unwrap_or_else(|| unreachable!());
            output.write(slot.take_output());
        }
        BatchOutput {
            outputs,
            index: 0,
            len,
        }
    }
}

#[pinned_drop]
impl<F, O, const N: usize> PinnedDrop for BatchCore<F, O, N> {
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

pub struct BatchOutput<O, const N: usize> {
    outputs: [MaybeUninit<O>; N],
    index: usize,
    len: usize,
}

struct BatchOutputDrop<'a, O> {
    outputs: &'a mut [MaybeUninit<O>],
}

impl<O> Drop for BatchOutputDrop<'_, O> {
    fn drop(&mut self) {
        while !self.outputs.is_empty() {
            let outputs = core::mem::take(&mut self.outputs);
            let (head, tail) = outputs.split_first_mut().unwrap_or_else(|| unreachable!());
            let mut remaining = Self { outputs: tail };
            // SAFETY: this guard owns exactly the initialized, unconsumed
            // suffix. `remaining` owns the tail if dropping `head` unwinds.
            unsafe { head.assume_init_drop() };
            self.outputs = core::mem::take(&mut remaining.outputs);
            forget(remaining);
        }
    }
}

impl<O, const N: usize> Iterator for BatchOutput<O, N> {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.len {
            return None;
        }
        let index = self.index;
        self.index += 1;
        // SAFETY: `index < len` identifies one initialized, unread element;
        // advancing first transfers its sole drop obligation to the caller.
        Some(unsafe { self.outputs[index].assume_init_read() })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.index;
        (remaining, Some(remaining))
    }
}

impl<O, const N: usize> ExactSizeIterator for BatchOutput<O, N> {}

impl<O, const N: usize> Drop for BatchOutput<O, N> {
    fn drop(&mut self) {
        drop(BatchOutputDrop {
            outputs: &mut self.outputs[self.index..self.len],
        });
    }
}
