use core::{pin, task};
use std::process;

use dope::core::driver::schedule;

pub(super) mod admission;
mod sealed;
pub(crate) use sealed::Binding;

use crate::{
    abi::{self, batch},
    context,
};

enum GroupDomain {}

/// Fixed-capacity streaming group for homogeneous fibers.
/// Wakes produced while polling enter the next stable batch.
#[pin_project::pin_project(!Unpin)]
pub struct Group<'d, F, const N: usize>
where
    F: abi::Fiber<'d>,
{
    #[pin]
    slots: [abi::Slot<F, F::Output>; N],
    #[pin]
    bindings: [crate::raw::Binding<'d>; N],
    #[pin]
    ready: batch::Queue<N>,
    domain: super::Domain<'d, GroupDomain, N>,
    free: [usize; N],
    free_len: usize,
    len: usize,
}

impl<'d, F, const N: usize> Group<'d, F, N>
where
    F: abi::Fiber<'d>,
{
    /// Reserves the complete ready-set storage before any member is admitted.
    pub fn try_new(parent: context::RootWaker<'d>) -> Result<Self, super::GroupAdmissionError> {
        use core::array::from_fn;

        const {
            assert!(N > 0, "fiber group capacity must be positive");
        }
        let domain = super::Domain::try_new(parent)?;
        Ok(Self {
            slots: from_fn(|_| abi::Slot::vacant()),
            bindings: from_fn(|_| crate::raw::Binding::new()),
            ready: batch::Queue::try_new()?,
            domain,
            free: from_fn(|index| N - index - 1),
            free_len: N,
            len: 0,
        })
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn is_full(&self) -> bool {
        self.len == N
    }

    /// Inserts one member without allocating and schedules its initial poll.
    pub fn try_push(self: pin::Pin<&mut Self>, fiber: F) -> Result<(), F> {
        let mut this = self.project();
        if *this.len == N {
            return Err(fiber);
        }
        let Some(next) = this.free_len.checked_sub(1) else {
            process::abort();
        };
        let raw = this.free[next];
        *this.free_len = next;
        let Some(index) = batch::Index::new(raw) else {
            process::abort();
        };
        let mut slot = batch::PinnedArray::at(this.slots.as_mut(), index);
        if slot.as_mut().fill(fiber).is_err() {
            process::abort();
        }
        if !this.ready.as_ref().return_ready(index) {
            process::abort();
        }
        *this.len += 1;
        this.domain.wake_parent();
        Ok(())
    }

    /// Polls one stable batch of exact ready members.
    /// Completed members are reclaimed before `complete` sees their output.
    pub fn drive_ready(
        self: pin::Pin<&mut Self>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
        mut complete: impl FnMut(F::Output),
    ) -> usize {
        let mut this = self.project();
        if *this.len == 0 || this.ready.is_empty() {
            return 0;
        }
        let Some(mut ready) = this.ready.as_ref().snapshot() else {
            process::abort();
        };
        let mut completed = 0;
        loop {
            let (index, permit) = match context.as_ref().admit_next(&mut ready) {
                schedule::ApplicationAdmission::Item(index, permit) => (index, permit),
                schedule::ApplicationAdmission::Empty => break,
                schedule::ApplicationAdmission::Exhausted(_) => {
                    ready.pause();
                    this.domain.wake_parent();
                    return completed;
                }
            };
            let binding = batch::PinnedArray::at(this.bindings.as_ref(), index);
            let wake = if binding.is_bound() {
                let Some(wake) = crate::raw::Binding::waker(Binding::new(binding)) else {
                    process::abort();
                };
                wake
            } else {
                let Some(wake) = crate::raw::Binding::bind_domain(
                    Binding::new(binding),
                    this.ready.as_ref(),
                    index.get(),
                    index,
                    this.domain.inner(),
                ) else {
                    process::abort();
                };
                wake
            };
            let mut slot = batch::PinnedArray::at(this.slots.as_mut(), index);
            let Some(fiber) = slot.as_mut().as_live() else {
                process::abort();
            };
            let outcome = context
                .as_mut()
                .poll_admitted_with_waker(fiber, wake, permit);
            let task::Poll::Ready(output) = outcome else {
                continue;
            };

            if !crate::raw::Binding::reclaim_domain(
                Binding::new(binding),
                index.get(),
                this.domain.inner(),
            ) {
                process::abort();
            }
            slot.as_mut().cancel();
            if *this.free_len >= N {
                process::abort();
            }
            this.free[*this.free_len] = index.get();
            *this.free_len += 1;
            *this.len -= 1;
            completed += 1;
            complete(output);
        }
        drop(ready);
        if !this.ready.is_empty() {
            context.wake();
        }
        completed
    }
}
