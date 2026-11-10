use core::{pin::Pin, task::Poll};

use crate::{
    abi,
    context::{self, PollCall, PollResult},
};

/// Owns a pinned fiber or stream until completion, then releases it and remains terminal.
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
        let (this, admit) = call.into_admit();
        match this.poll_with(|fiber| {
            // SAFETY: Task owns the live slot and vacates it on completion.
            PollResult::Polled(unsafe { context::raw::Live::new(fiber) }.admitted(admit))
        }) {
            PollResult::Polled(poll) => poll,
            PollResult::Exhausted => Poll::Pending,
        }
    }
}

impl<'d, S: abi::Stream<'d>> abi::Stream<'d> for Task<S> {
    type Item = S::Item;

    fn poll_next(call: PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>> {
        let (this, admit) = call.into_admit();
        let mut state = this.project().state;
        let Some(stream) = state.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        // SAFETY: Task owns the live stream and drops it on EOF.
        let result = unsafe { context::raw::Live::new(stream) }.admitted_next(admit);
        if matches!(result, Poll::Ready(None)) {
            state.set(None);
        }
        result
    }
}
