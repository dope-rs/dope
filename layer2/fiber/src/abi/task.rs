use core::{pin::Pin, task::Poll};

use crate::{
    abi,
    context::{self, PollCall, PollResult},
    task::storage::slots::{self, Action},
};

/// Owns a fiber until completion, then releases it and remains terminal.
#[pin_project::pin_project]
#[repr(transparent)]
#[must_use = "a task does nothing unless it is driven"]
pub struct Task<F> {
    #[pin]
    state: slots::raw::Entry<F, ()>,
}

impl<F> Task<F> {
    pub const fn new(fiber: F) -> Self {
        Self {
            state: slots::raw::Entry::live(fiber),
        }
    }
    pub(crate) fn poll_with<'d>(
        self: Pin<&mut Self>,
        poll: impl FnOnce(Pin<&mut F>) -> PollResult<F::Output>,
    ) -> PollResult<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        match self
            .project()
            .state
            .visit((), |fiber, ()| match poll(fiber) {
                result @ PollResult::Polled(Poll::Ready(_)) => Action::Vacate(result),
                result => Action::Keep(result),
            }) {
            slots::Phase::Live(result) => result,
            slots::Phase::Vacant(()) | slots::Phase::Done(()) => PollResult::Polled(Poll::Pending),
        }
    }
}

impl<'d, F: abi::Fiber<'d>> abi::Fiber<'d> for Task<F> {
    type Output = F::Output;
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        match this.poll_with(|fiber| {
            // SAFETY: this call owns the sole initial descent into its live slot.
            PollResult::Polled(unsafe { context::raw::Live::new(fiber).delegate(context) })
        }) {
            PollResult::Polled(poll) => poll,
            PollResult::Exhausted => Poll::Pending,
        }
    }
}
