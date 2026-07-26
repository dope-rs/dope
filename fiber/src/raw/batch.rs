use core::array::from_fn;
use core::marker::PhantomPinned;
use core::mem::{ManuallyDrop, MaybeUninit, forget};
use core::pin::{Pin, pin};
use core::task::Poll;

use crate::abi::Fiber;
use crate::raw::task::queue::IndexQueue;
use crate::raw::task::{Context, TaskContext};

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Vacant,
    Live,
    Done,
}

union SlotValue<F, O> {
    fiber: ManuallyDrop<F>,
    output: ManuallyDrop<O>,
}

struct Slot<F, O> {
    value: MaybeUninit<SlotValue<F, O>>,
    state: SlotState,
}

impl<F, O> Slot<F, O> {
    const fn new() -> Self {
        Self {
            value: MaybeUninit::uninit(),
            state: SlotState::Vacant,
        }
    }

    fn write(&mut self, fiber: F) {
        debug_assert!(self.state == SlotState::Vacant);
        self.value.write(SlotValue {
            fiber: ManuallyDrop::new(fiber),
        });
        self.state = SlotState::Live;
    }

    fn poll<'d>(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<O>
    where
        F: Fiber<'d, Output = O>,
    {
        debug_assert!(self.state == SlotState::Live);
        // SAFETY: pinning the slot pins its active union field. `Live` proves
        // that field is an initialized `F`, and this method never moves it.
        let this = unsafe { self.get_unchecked_mut() };
        let value = unsafe { this.value.assume_init_mut() };
        let fiber = unsafe { &mut *value.fiber };
        Fiber::poll(unsafe { Pin::new_unchecked(fiber) }, context)
    }

    fn complete(self: Pin<&mut Self>, output: O) {
        debug_assert!(self.state == SlotState::Live);
        // SAFETY: `Live` proves the active union field is an initialized,
        // pinned `F`. The transaction installs `output` even if dropping that
        // fiber unwinds, so the old field is never dropped twice.
        let this = unsafe { self.get_unchecked_mut() };
        let slot = this as *mut Self;
        let value = unsafe { this.value.assume_init_mut() };
        let fiber = unsafe { &mut value.fiber } as *mut ManuallyDrop<F>;
        this.state = SlotState::Vacant;
        let transaction = CompletionTransaction {
            slot,
            output: Some(output),
        };
        unsafe { ManuallyDrop::drop(&mut *fiber) };
        transaction.finish();
    }

    fn take_output(&mut self) -> O {
        debug_assert!(self.state == SlotState::Done);
        self.state = SlotState::Vacant;
        // SAFETY: `Done` proves the active union field is an initialized `O`.
        // Marking it vacant transfers the sole drop obligation to the caller.
        let value = unsafe { self.value.assume_init_mut() };
        unsafe { ManuallyDrop::take(&mut value.output) }
    }

    fn is_live(&self) -> bool {
        self.state == SlotState::Live
    }

    fn install_output(&mut self, output: O) {
        debug_assert!(self.state == SlotState::Vacant);
        self.value.write(SlotValue {
            output: ManuallyDrop::new(output),
        });
        self.state = SlotState::Done;
    }
}

impl<F, O> Drop for Slot<F, O> {
    fn drop(&mut self) {
        match self.state {
            SlotState::Vacant => {}
            SlotState::Live => {
                // SAFETY: `Live` selects the initialized fiber union field.
                let value = unsafe { self.value.assume_init_mut() };
                unsafe { ManuallyDrop::drop(&mut value.fiber) };
            }
            SlotState::Done => {
                // SAFETY: `Done` selects the initialized output union field.
                let value = unsafe { self.value.assume_init_mut() };
                unsafe { ManuallyDrop::drop(&mut value.output) };
            }
        }
    }
}

struct CompletionTransaction<F, O> {
    slot: *mut Slot<F, O>,
    output: Option<O>,
}

impl<F, O> CompletionTransaction<F, O> {
    fn finish(mut self) {
        self.install();
    }

    fn install(&mut self) {
        let Some(output) = self.output.take() else {
            return;
        };
        // SAFETY: the transaction cannot outlive the `complete` call that
        // created it, so `slot` still names that exclusively borrowed slot.
        unsafe { &mut *self.slot }.install_output(output);
    }
}

impl<F, O> Drop for CompletionTransaction<F, O> {
    fn drop(&mut self) {
        self.install();
    }
}

pub(crate) enum PollStep {
    Idle,
    Pending,
    Ready,
}

pub(crate) struct BatchCore<F, O, const N: usize> {
    slots: [Slot<F, O>; N],
    tasks: [TaskContext; N],
    ready: IndexQueue,
    len: usize,
    next_bind: usize,
    _pin: PhantomPinned,
}

impl<F, O, const N: usize> BatchCore<F, O, N> {
    pub(crate) fn new() -> Self {
        Self {
            slots: from_fn(|_| Slot::new()),
            tasks: from_fn(|_| TaskContext::new()),
            ready: IndexQueue::with_capacity(N),
            len: 0,
            next_bind: 0,
            _pin: PhantomPinned,
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
        // SAFETY: the core is pinned for this poll. All projections below stay
        // within it, and no slot, task context, or queue is moved.
        let this = unsafe { self.get_unchecked_mut() };
        let (index, wake) = if let Some(index) = unsafe { Pin::new_unchecked(&this.ready) }.pop() {
            let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
            (index, unsafe { task.context_unchecked() })
        } else if this.next_bind < this.len {
            let index = this.next_bind;
            this.next_bind += 1;
            // SAFETY: the child fiber is owned by this pinned core and Drop
            // unbinds its task before either the fiber or queue disappears.
            let parent = unsafe { context.waker_unchecked() };
            let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
            let queue = unsafe { Pin::new_unchecked(&this.ready) };
            let wake = unsafe { task.bind_index(queue, index, parent) };
            (index, wake)
        } else {
            return PollStep::Idle;
        };

        let mut child = pin!(Context::from_waker(wake, context.as_mut().driver_access()));
        let mut slot = unsafe { Pin::new_unchecked(&mut this.slots[index]) };
        let Poll::Ready(output) = slot.as_mut().poll(child.as_mut()) else {
            return PollStep::Pending;
        };
        let task = unsafe { Pin::new_unchecked(&this.tasks[index]) };
        unsafe { task.unbind() };
        slot.complete(output);
        PollStep::Ready
    }

    pub(crate) fn has_work(self: Pin<&Self>) -> bool {
        let this = self.get_ref();
        this.next_bind < this.len || !this.ready.is_empty()
    }

    pub(crate) fn take_output(self: Pin<&mut Self>) -> BatchOutput<O, N> {
        // SAFETY: completion has unbound every task and changed every occupied
        // slot to `Done`; moving outputs cannot move a pinned live fiber.
        let this = unsafe { self.get_unchecked_mut() };
        let mut outputs = from_fn(|_| MaybeUninit::uninit());
        for (output, slot) in outputs[..this.len].iter_mut().zip(&mut this.slots) {
            output.write(slot.take_output());
        }
        BatchOutput {
            outputs,
            index: 0,
            len: this.len,
        }
    }
}

impl<F, O, const N: usize> Drop for BatchCore<F, O, N> {
    fn drop(&mut self) {
        for index in 0..self.next_bind {
            if self.slots[index].is_live() && self.tasks[index].is_bound() {
                // SAFETY: a core only binds after it is pinned, so its task
                // array remains at the same address until this Drop completes.
                let task = unsafe { Pin::new_unchecked(&self.tasks[index]) };
                unsafe { task.unbind() };
            }
        }
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
            // SAFETY: `[index, len)` is exactly the initialized, unconsumed
            // suffix. `remaining` owns the later suffix if this drop unwinds.
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
            outputs: self.outputs.as_mut_ptr(),
            index: self.index,
            len: self.len,
        });
    }
}
