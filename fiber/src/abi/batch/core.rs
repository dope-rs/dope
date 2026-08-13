use std::process;

use ::core::{pin, task};
use dope::core::driver::schedule;
use o3::collections::{self, fixed::array};

use crate::{
    abi::{self, slot},
    context,
};

enum SlotPoll {
    Pending,
    Ready,
}

fn poll_slot<'turn, 'd: 'turn, F, O, const N: usize>(
    mut slots: pin::Pin<&mut [slot::Slot<F, O>; N]>,
    tasks: pin::Pin<&[crate::raw::Binding<'d>; N]>,
    mut context: pin::Pin<&mut context::Context<'turn, 'd>>,
    index: super::Index<N>,
    wake: context::Waker<'d>,
    permit: schedule::ApplicationPermit<'turn, 'd>,
    domain: &mut super::Domain<'d, N>,
) -> SlotPoll
where
    F: abi::Fiber<'d, Output = O>,
{
    let mut slot = super::PinnedArray::at(slots.as_mut(), index);
    let Some(fiber) = slot.as_mut().as_live() else {
        process::abort();
    };
    let poll = context
        .as_mut()
        .poll_admitted_with_waker(fiber, wake, permit);
    let task::Poll::Ready(output) = poll else {
        return SlotPoll::Pending;
    };
    let task = super::PinnedArray::at(tasks, index);
    if !crate::raw::Binding::reclaim_domain(
        super::Task::new(task),
        index.get(),
        domain.inner().inner(),
    ) {
        process::abort();
    }
    slot.complete(output);
    SlotPoll::Ready
}

#[pin_project::pin_project(!Unpin)]
pub(crate) struct Core<'d, F, O, const N: usize> {
    #[pin]
    slots: [slot::Slot<F, O>; N],
    #[pin]
    tasks: [crate::raw::Binding<'d>; N],
    #[pin]
    ready: super::Queue<N>,
    len: usize,
    remaining: usize,
    next_bind: usize,
}

impl<'d, F, O, const N: usize> Core<'d, F, O, N> {
    pub(crate) fn try_empty() -> Result<Self, collections::AllocationError> {
        use ::core::array::from_fn;

        let ready = super::Queue::try_new()?;
        Ok(Self::from_slots(
            from_fn(|_| slot::Slot::vacant()),
            ready,
            0,
        ))
    }

    pub(crate) fn try_from_array(fibers: [F; N]) -> Result<Self, collections::AllocationError> {
        let ready = super::Queue::try_new()?;
        Ok(Self::from_slots(fibers.map(slot::Slot::live), ready, N))
    }

    fn from_slots(slots: [slot::Slot<F, O>; N], ready: super::Queue<N>, len: usize) -> Self {
        use ::core::array::from_fn;

        Self {
            slots,
            tasks: from_fn(|_| crate::raw::Binding::new()),
            ready,
            len,
            remaining: len,
            next_bind: 0,
        }
    }

    pub(crate) fn try_push(&mut self, fiber: F) -> Result<(), F> {
        if self.next_bind != 0 || self.len == N {
            return Err(fiber);
        }
        self.slots[self.len].write(fiber);
        self.len += 1;
        self.remaining += 1;
        Ok(())
    }

    pub(crate) fn drive(
        self: pin::Pin<&mut Self>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
        domain: &mut super::Domain<'d, N>,
    ) -> task::Poll<()>
    where
        F: abi::Fiber<'d, Output = O>,
    {
        let mut this = self.project();
        if *this.remaining == 0 {
            return task::Poll::Ready(());
        }

        let mut budget = super::POLL_BUDGET;
        while budget != 0 && *this.next_bind < *this.len {
            let Some(permit) = context.as_ref().admit() else {
                context.wake();
                return task::Poll::Pending;
            };
            if *this.next_bind == 0 && !domain.retarget(context.as_ref().root_waker()) {
                process::abort();
            }
            let Some(index) = super::Index::new(*this.next_bind) else {
                process::abort();
            };
            let task = super::PinnedArray::at(this.tasks.as_ref(), index);
            let Some(wake) = crate::raw::Binding::bind_domain(
                super::Task::new(task),
                this.ready.as_ref(),
                index.get(),
                index,
                domain.inner().inner(),
            ) else {
                process::abort();
            };
            *this.next_bind += 1;
            budget -= 1;
            match poll_slot(
                this.slots.as_mut(),
                this.tasks.as_ref(),
                context.as_mut(),
                index,
                wake,
                permit,
                domain,
            ) {
                SlotPoll::Pending => {}
                SlotPoll::Ready => {
                    *this.remaining -= 1;
                    if *this.remaining == 0 {
                        return task::Poll::Ready(());
                    }
                }
            }
        }

        if budget == 0 {
            if *this.next_bind < *this.len || !this.ready.is_empty() {
                context.wake();
            }
            return task::Poll::Pending;
        }

        let Some(mut ready) = this.ready.as_ref().snapshot() else {
            return task::Poll::Pending;
        };
        while budget != 0 {
            let (index, permit) = match context.as_ref().admit_next(&mut ready) {
                schedule::ApplicationAdmission::Item(index, permit) => (index, permit),
                schedule::ApplicationAdmission::Empty => {
                    drop(ready);
                    if !this.ready.is_empty() {
                        context.wake();
                    }
                    return task::Poll::Pending;
                }
                schedule::ApplicationAdmission::Exhausted(_) => {
                    ready.pause();
                    context.wake();
                    return task::Poll::Pending;
                }
            };
            let task = super::PinnedArray::at(this.tasks.as_ref(), index);
            let Some(wake) = crate::raw::Binding::waker(super::Task::new(task)) else {
                process::abort();
            };
            budget -= 1;
            match poll_slot(
                this.slots.as_mut(),
                this.tasks.as_ref(),
                context.as_mut(),
                index,
                wake,
                permit,
                domain,
            ) {
                SlotPoll::Pending => {}
                SlotPoll::Ready => {
                    *this.remaining -= 1;
                    if *this.remaining == 0 {
                        return task::Poll::Ready(());
                    }
                }
            }
        }

        ready.pause();
        if !this.ready.is_empty() {
            context.wake();
        }
        task::Poll::Pending
    }

    pub(crate) fn take_output(self: pin::Pin<&mut Self>) -> array::IntoIter<O, N> {
        use o3::collections::fixed::array::Inline;
        let mut this = self.project();
        let len = *this.len;
        Inline::from_fn(len, |index| {
            let Some(index) = super::Index::new(index) else {
                process::abort();
            };
            let slot = super::PinnedArray::at(this.slots.as_mut(), index);
            slot.take_output()
        })
        .into_iter()
    }

    pub(crate) fn cancel(self: pin::Pin<&mut Self>, domain: &mut super::Domain<'d, N>) {
        let mut this = self.project();
        for index in 0..*this.next_bind {
            let Some(index) = super::Index::new(index) else {
                process::abort();
            };
            let task = super::PinnedArray::at(this.tasks.as_ref(), index);
            if task.is_bound()
                && !crate::raw::Binding::reclaim_domain(
                    super::Task::new(task),
                    index.get(),
                    domain.inner().inner(),
                )
            {
                process::abort();
            }
            let mut slot = super::PinnedArray::at(this.slots.as_mut(), index);
            if slot.is_live() {
                slot.as_mut().cancel();
            }
        }
    }
}
