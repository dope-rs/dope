use core::array::from_fn;
use core::marker::PhantomPinned;
use core::mem::MaybeUninit;
use core::mem::forget;
use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::task::queue::IndexQueue;
use crate::{Context, TaskContext};

const BATCH_POLL_BUDGET: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchSlotState {
    Vacant,
    Live,
    Done,
}

struct BatchSlot<F, O> {
    fiber: MaybeUninit<F>,
    output: MaybeUninit<O>,
    state: BatchSlotState,
}

impl<F, O> BatchSlot<F, O> {
    const fn new() -> Self {
        Self {
            fiber: MaybeUninit::uninit(),
            output: MaybeUninit::uninit(),
            state: BatchSlotState::Vacant,
        }
    }

    fn write(&mut self, fiber: F) {
        debug_assert!(self.state == BatchSlotState::Vacant);
        self.fiber.write(fiber);
        self.state = BatchSlotState::Live;
    }

    fn live_mut(&mut self) -> &mut F {
        debug_assert!(self.state == BatchSlotState::Live);
        unsafe { self.fiber.assume_init_mut() }
    }

    fn complete(&mut self, output: O) {
        debug_assert!(self.state == BatchSlotState::Live);
        self.output.write(output);
        self.state = BatchSlotState::Done;
        unsafe { self.fiber.assume_init_drop() };
    }

    fn take_output(&mut self) -> O {
        debug_assert!(self.state == BatchSlotState::Done);
        self.state = BatchSlotState::Vacant;
        unsafe { self.output.assume_init_read() }
    }

    fn is_live(&self) -> bool {
        self.state == BatchSlotState::Live
    }
}

impl<F, O> Drop for BatchSlot<F, O> {
    fn drop(&mut self) {
        match self.state {
            BatchSlotState::Vacant => {}
            BatchSlotState::Live => unsafe { self.fiber.assume_init_drop() },
            BatchSlotState::Done => unsafe { self.output.assume_init_drop() },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchStatus {
    Idle,
    Polling,
    Completed,
    Poisoned,
}

struct BatchTransaction(*mut BatchStatus, bool);

impl BatchTransaction {
    fn finish(mut self, status: BatchStatus) {
        unsafe { *self.0 = status };
        self.1 = false;
    }
}

impl Drop for BatchTransaction {
    fn drop(&mut self) {
        if self.1 {
            unsafe { *self.0 = BatchStatus::Poisoned };
        }
    }
}

pub struct Batch<F, O, const N: usize> {
    slots: [BatchSlot<F, O>; N],
    tasks: [TaskContext<u16>; N],
    ready: IndexQueue,
    len: usize,
    next_bind: usize,
    remaining: usize,
    status: BatchStatus,
    _pin: PhantomPinned,
}

impl<F, O, const N: usize> Batch<F, O, N> {
    pub fn new() -> Self {
        const {
            assert!(N > 0, "batch capacity must be > 0");
            assert!(
                N <= u16::MAX as usize,
                "batch capacity exceeds ready queue target"
            );
        }
        Self {
            slots: from_fn(|_| BatchSlot::new()),
            tasks: from_fn(|index| TaskContext::with_target(index as u16)),
            ready: IndexQueue::with_capacity(N),
            len: 0,
            next_bind: 0,
            remaining: 0,
            status: BatchStatus::Idle,
            _pin: PhantomPinned,
        }
    }

    pub fn from_array(fibers: [F; N]) -> Self {
        let mut batch = Self::new();
        for fiber in fibers {
            if batch.try_push(fiber).is_err() {
                unreachable!()
            }
        }
        batch
    }

    pub fn try_push(&mut self, fiber: F) -> Result<(), F> {
        if self.status != BatchStatus::Idle || self.next_bind != 0 || self.len == N {
            return Err(fiber);
        }
        self.slots[self.len].write(fiber);
        self.len += 1;
        self.remaining += 1;
        Ok(())
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn take_output(&mut self) -> BatchOutput<O, N> {
        let mut outputs = from_fn(|_| MaybeUninit::uninit());
        for (output, slot) in outputs[..self.len].iter_mut().zip(&mut self.slots) {
            output.write(slot.take_output());
        }
        BatchOutput {
            outputs,
            index: 0,
            len: self.len,
        }
    }
}

impl<F, O, const N: usize> Default for Batch<F, O, N> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BatchOutput<O, const N: usize> {
    outputs: [MaybeUninit<O>; N],
    index: usize,
    len: usize,
}

struct BatchOutputDrop<O> {
    outputs: *mut MaybeUninit<O>,
    index: usize,
    len: usize,
}

impl<O> Drop for BatchOutputDrop<O> {
    fn drop(&mut self) {
        while self.index < self.len {
            let index = self.index;
            self.index += 1;
            let remaining = Self {
                outputs: self.outputs,
                index: self.index,
                len: self.len,
            };
            unsafe { (*self.outputs.add(index)).assume_init_drop() };
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
            outputs: self.outputs.as_mut_ptr(),
            index: self.index,
            len: self.len,
        });
    }
}

impl<'d, F, O, const N: usize> Fiber<'d> for Batch<F, O, N>
where
    F: Fiber<'d, Output = O>,
{
    type Output = BatchOutput<O, N>;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match this.status {
            BatchStatus::Completed => panic!("batch polled after completion"),
            BatchStatus::Polling => panic!("batch polled reentrantly"),
            BatchStatus::Poisoned => panic!("batch polled after panic"),
            BatchStatus::Idle => {}
        }
        this.status = BatchStatus::Polling;
        let transaction = BatchTransaction(&mut this.status, true);
        if this.remaining == 0 {
            transaction.finish(BatchStatus::Completed);
            return Poll::Ready(this.take_output());
        }

        for _ in 0..BATCH_POLL_BUDGET {
            let (index, mut child_context) =
                if let Some(index) = unsafe { Pin::new_unchecked(&this.ready) }.pop() {
                    let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
                    let wake = unsafe { task.context_unchecked() };
                    (index, cx.as_mut().child(wake))
                } else if this.next_bind < this.len {
                    let index = this.next_bind;
                    this.next_bind += 1;
                    let parent = unsafe { cx.waker_unchecked() };
                    let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
                    let queue = unsafe { Pin::new_unchecked(&this.ready) };
                    let wake = unsafe { task.bind_index(queue, index, index as u16, parent) };
                    (index, cx.as_mut().child(wake))
                } else {
                    transaction.finish(BatchStatus::Idle);
                    return Poll::Pending;
                };

            let mut child_context = unsafe { Pin::new_unchecked(&mut child_context) };
            let fiber = this.slots[index].live_mut();
            let output = Fiber::poll(unsafe { Pin::new_unchecked(fiber) }, child_context.as_mut());
            if let Poll::Ready(output) = output {
                let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
                unsafe { task.unbind() };
                this.remaining -= 1;
                this.slots[index].complete(output);
                if this.remaining == 0 {
                    transaction.finish(BatchStatus::Completed);
                    return Poll::Ready(this.take_output());
                }
            }
        }

        let ready = unsafe { Pin::new_unchecked(&this.ready) };
        let has_work = this.next_bind < this.len || !ready.is_empty();
        transaction.finish(BatchStatus::Idle);
        if has_work {
            cx.wake();
        }
        Poll::Pending
    }
}

impl<F, O, const N: usize> Drop for Batch<F, O, N> {
    fn drop(&mut self) {
        for index in 0..self.next_bind {
            if self.slots[index].is_live() {
                let task = unsafe { Pin::new_unchecked(&self.tasks[index]) };
                unsafe { task.unbind() };
            }
        }
    }
}
