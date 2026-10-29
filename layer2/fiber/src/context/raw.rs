use core::{pin::Pin, task::Poll};

use crate::{
    abi,
    context::{Context, PollCall, PollResult, wake},
    core::driver::schedule::app,
};

/// One unfinished pinned fiber retained until its admitted poll returns.
#[repr(transparent)]
pub struct Live<'a, F> {
    fiber: Pin<&'a mut F>,
}

impl<'a, F> Live<'a, F> {
    /// # Safety
    /// The owner must retain this unfinished pinned fiber; Ready must end its
    /// polling authority before another Live can be constructed for it.
    pub unsafe fn new(fiber: Pin<&'a mut F>) -> Self {
        Self { fiber }
    }

    pub fn poll<'turn, 'd: 'turn>(self, context: Pin<&mut Context<'turn, 'd>>) -> Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        match self.try_poll(context) {
            PollResult::Polled(poll) => poll,
            PollResult::Exhausted => Poll::Pending,
        }
    }

    pub fn try_poll<'turn, 'd: 'turn>(
        self,
        context: Pin<&mut Context<'turn, 'd>>,
    ) -> PollResult<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        PollCall::try_poll(self.fiber, context, F::poll)
    }

    /// # Safety
    /// Transfer an existing PollCall's admission to this live child exactly once.
    pub(crate) unsafe fn delegate<'turn, 'd: 'turn>(
        self,
        context: Pin<&mut Context<'turn, 'd>>,
    ) -> Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        F::poll(PollCall {
            fiber: self.fiber,
            context,
        })
    }

    pub(crate) fn with<'turn, 'd: 'turn>(
        self,
        context: Pin<&mut Context<'turn, 'd>>,
        permit: app::Permit<'turn, 'd>,
    ) -> Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        permit.run(|| {
            F::poll(PollCall {
                fiber: self.fiber,
                context,
            })
        })
    }

    /// # Safety
    /// Transfer an existing PollCall's admission to this live child exactly once.
    pub(crate) unsafe fn child<'turn, 'd: 'turn>(
        self,
        context: Pin<&mut Context<'turn, 'd>>,
        wake: wake::Waker<'d>,
    ) -> Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        context.with_child(wake, |child| {
            // SAFETY: the caller transfers its existing admission to this child.
            unsafe { self.delegate(child) }
        })
    }

    pub(crate) fn child_with<'turn, 'd: 'turn>(
        self,
        context: Pin<&mut Context<'turn, 'd>>,
        wake: wake::Waker<'d>,
        permit: app::Permit<'turn, 'd>,
    ) -> Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        context.with_child(wake, |child| self.with(child, permit))
    }
}
