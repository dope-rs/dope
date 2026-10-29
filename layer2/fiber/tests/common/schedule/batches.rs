#![deny(unsafe_code)]

use std::{cell::Cell, task::Poll};

use dope::core::driver::schedule::ready::wake;
use dope_fiber::{abi::Fiber, context::PollCall, wait, wait::Waiter};

pub struct FanIn<'cells, 'd, const N: usize> {
    pub(crate) index: usize,
    pub(crate) polls: &'cells Cell<Option<usize>>,
    pub(crate) ready: &'cells Cell<bool>,
    pub(crate) wakes: &'cells Cell<Vec<wake::Waker<'d>>>,
}

impl<'cells, 'd, const N: usize> Fiber<'d> for FanIn<'cells, 'd, N> {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        this.polls
            .set(this.polls.get().and_then(|count| count.checked_add(1)));
        if this.ready.get() {
            return Poll::Ready(this.index);
        }

        let mut wakes = this.wakes.take();
        wakes.push(context.as_ref().completion());
        if wakes.len() == N {
            this.ready.set(true);
            for wake in wakes.drain(..) {
                wake.wake();
            }
        }
        this.wakes.set(wakes);
        Poll::Pending
    }
}

#[pin_project::pin_project]
pub struct ParkedChild<'target, 'd> {
    pub(crate) follower: bool,
    pub(crate) slot: &'target wait::Slot<'d>,
    pub(crate) ready: &'target Cell<bool>,
    pub(crate) polls: &'target Cell<Option<usize>>,
    #[pin]
    pub(crate) waiter: Waiter<'target, 'd>,
}

impl<'target, 'd> Fiber<'d> for ParkedChild<'target, 'd> {
    type Output = Result<usize, wait::Error<'d>>;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.project();
        let index = usize::from(!*this.follower);
        this.polls
            .set(this.polls.get().and_then(|count| count.checked_add(1)));

        if !*this.follower {
            this.ready.set(true);
            this.slot.wake();
            return Poll::Ready(Ok(index));
        }
        if this.ready.get() {
            this.waiter.as_ref().unregister();
            return Poll::Ready(Ok(index));
        }
        if let Err(error) = this
            .slot
            .try_register(this.waiter.as_ref(), context.as_ref())
        {
            return Poll::Ready(Err(error));
        }
        Poll::Pending
    }
}
