use core::{pin::Pin, task::Poll};

use crate::{
    abi,
    context::{self, PollCall, PollResult},
};

/// Owns a fiber until completion, then releases it and remains terminal.
#[pin_project::pin_project]
#[repr(transparent)]
#[must_use = "a task does nothing unless it is driven"]
pub struct Task<F> {
    #[pin]
    state: Option<F>,
}

impl<F> Task<F> {
    pub const fn new(fiber: F) -> Self {
        Self { state: Some(fiber) }
    }
    pub(crate) fn poll_with<'d>(
        self: Pin<&mut Self>,
        poll: impl FnOnce(Pin<&mut F>) -> PollResult<F::Output>,
    ) -> PollResult<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        let mut state = self.project().state;
        let Some(fiber) = state.as_mut().as_pin_mut() else {
            return PollResult::Polled(Poll::Pending);
        };
        let result = poll(fiber);
        if matches!(result, PollResult::Polled(Poll::Ready(_))) {
            state.set(None);
        }
        result
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
