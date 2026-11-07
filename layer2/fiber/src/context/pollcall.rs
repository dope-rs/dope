use core::{pin::Pin, task::Poll};

use crate::{
    context::{Admission, Context, Continue, PollResult},
    core::driver::schedule::app::step,
};

/// Exact, single-use admission for polling one pinned fiber.
/// It is constructed only after consuming one application-work credit and
/// cannot be retagged to a different fiber type.
pub struct PollCall<'call, 'turn, 'd: 'turn, F> {
    pub(super) fiber: Pin<&'call mut F>,
    pub(super) context: Pin<&'call mut Context<'turn, 'd>>,
}

impl<'call, 'turn, 'd: 'turn, F> PollCall<'call, 'turn, 'd, F> {
    pub(crate) fn try_poll<T>(
        fiber: Pin<&'call mut F>,
        context: Pin<&'call mut Context<'turn, 'd>>,
        poll: impl FnOnce(Self) -> Poll<T>,
    ) -> PollResult<T> {
        let Continue::Permit(_permit) = context.as_ref().try_continue() else {
            return PollResult::Exhausted;
        };
        PollResult::Polled(poll(Self { fiber, context }))
    }

    pub(crate) fn delegated<'permit>(
        fiber: Pin<&'call mut F>,
        context: Pin<&'call mut Context<'turn, 'd>>,
        _permit: step::Delegated<'permit, 'turn, 'd>,
    ) -> Self {
        Self { fiber, context }
    }

    pub fn into_fiber(self) -> Pin<&'call mut F> {
        self.fiber
    }

    pub fn into_context(self) -> Pin<&'call mut Context<'turn, 'd>> {
        self.context
    }

    pub fn into_parts(self) -> (Pin<&'call mut F>, Pin<&'call mut Context<'turn, 'd>>) {
        (self.fiber, self.context)
    }

    /// Retains this call's credit for one child task while lending the owner
    /// for its pinned projection and state transition.
    pub fn into_admit(self) -> (Pin<&'call mut F>, Admission<'call, 'turn, 'd>) {
        (
            self.fiber,
            Admission {
                context: self.context,
            },
        )
    }
}
