use std::{cell::Cell, pin::Pin, ptr, ptr::NonNull, rc::Rc, task::Poll};

use dope_fiber::{abi::Fiber, context::PollCall};

pub struct Domain;

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Pinned {
    pub(crate) address: Cell<Option<ptr::NonNull<Self>>>,
    pub(crate) observed: Rc<Cell<(usize, bool)>>,
}

impl Fiber<'_> for Pinned {
    type Output = ();

    fn poll(call: PollCall<'_, '_, '_, Self>) -> Poll<Self::Output> {
        let fiber = call.into_fiber();
        let address = NonNull::from(fiber.as_ref().get_ref());
        let this = fiber.project();
        if let Some(bound) = this.address.replace(Some(address)) {
            let (drops, stable) = this.observed.get();
            this.observed.set((drops, stable && bound == address));
        }
        Poll::Pending
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Pinned {
    fn drop(self: Pin<&mut Self>) {
        let address = NonNull::from(self.as_ref().get_ref());
        let this = self.project();
        let (drops, stable) = this.observed.get();
        this.observed
            .set((drops + 1, stable && this.address.get() == Some(address)));
    }
}

pub struct CompleteOnce {
    pub(crate) polls: Rc<Cell<usize>>,
}

impl<'d> Fiber<'d> for CompleteOnce {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let this = call.into_fiber();
        this.polls.set(this.polls.get() + 1);
        Poll::Ready(41)
    }
}

/// Capacity deferral observes readiness again without polling the completed fiber or moving output.
pub struct DropOutput {
    pub(crate) drops: Rc<Cell<usize>>,
}

impl Drop for DropOutput {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

pub struct DropReady {
    pub(crate) output_drops: Rc<Cell<usize>>,
}

impl<'d> Fiber<'d> for DropReady {
    type Output = DropOutput;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let this = call.into_fiber();
        Poll::Ready(DropOutput {
            drops: Rc::clone(&this.output_drops),
        })
    }
}
