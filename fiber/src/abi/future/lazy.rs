use core::task;

use crate::{abi, context};

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Lazy<F, Fb> {
    #[pin]
    state: State<F, Fb>,
}

#[pin_project::pin_project(project = StateProj)]
enum State<F, Fb> {
    Pending(Option<F>),
    Active(#[pin] Fb),
}

impl<F, Fb> Lazy<F, Fb> {
    pub const fn new(factory: F) -> Self {
        Self {
            state: State::Pending(Some(factory)),
        }
    }
}

impl<'d, F, Fb> abi::Fiber<'d> for Lazy<F, Fb>
where
    F: FnOnce() -> Fb,
    Fb: abi::Fiber<'d>,
{
    type Output = Fb::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::Poll;

        let (this, context) = call.into_parts();
        let mut state = this.project().state;
        match state.as_mut().project() {
            StateProj::Active(fiber) => context.try_poll(fiber).unwrap_or(Poll::Pending),
            StateProj::Pending(factory) => {
                let Some(permit) = context.as_ref().admit() else {
                    context.wake();
                    return Poll::Pending;
                };
                let Some(factory) = factory.take() else {
                    return Poll::Pending;
                };
                state.set(State::Active(factory()));
                match state.project() {
                    StateProj::Active(fiber) => context.poll_admitted(fiber, permit),
                    StateProj::Pending(_) => Poll::Pending,
                }
            }
        }
    }
}
