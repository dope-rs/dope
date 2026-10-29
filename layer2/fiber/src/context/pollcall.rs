use core::{pin::Pin, task::Poll};

use crate::{
    context::{Admission, Context, PollResult},
    core::driver::schedule::app::step,
};

/// Exact, single-use admission for polling one pinned fiber.
/// It is constructed only after consuming one application-work credit and
/// cannot be retagged to a different fiber type:
///
/// ```compile_fail
/// use dope_fiber::{
///     abi::{Pending, Ready},
///     context::PollCall,
/// };
///
/// fn retag<'call, 'turn, 'd: 'turn>(
///     call: PollCall<'call, 'turn, 'd, Ready<()>>,
/// ) -> PollCall<'call, 'turn, 'd, Pending<()>> {
///     call
/// }
/// ```
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
        let Some(permit) = context.as_ref().get_ref().work.permit() else {
            context.as_ref().get_ref().requeue();
            return PollResult::Exhausted;
        };
        PollResult::Polled(permit.run(|| poll(Self { fiber, context })))
    }

    pub(crate) fn map<T: 'call>(
        self,
        project: impl FnOnce(Pin<&'call mut F>) -> Pin<&'call mut T>,
    ) -> PollCall<'call, 'turn, 'd, T> {
        PollCall {
            fiber: project(self.fiber),
            context: self.context,
        }
    }

    pub(crate) fn delegated<'permit, R>(
        fiber: Pin<&'call mut F>,
        context: Pin<&'call mut Context<'turn, 'd>>,
        permit: step::Delegated<'permit, 'turn, 'd>,
        poll: impl FnOnce(Self) -> R,
    ) -> R {
        permit.run(|| poll(Self { fiber, context }))
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
