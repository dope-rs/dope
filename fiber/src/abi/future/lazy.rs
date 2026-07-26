use core::pin::Pin;
use core::task::Poll;

use pin_project::pin_project;

use super::super::Fiber;
use crate::raw::task::Context;

#[pin_project]
pub struct Lazy<F, Fb> {
    #[pin]
    state: State<F, Fb>,
}

#[pin_project(project = StateProj)]
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

impl<'d, F, Fb> Fiber<'d> for Lazy<F, Fb>
where
    F: FnOnce() -> Fb,
    Fb: Fiber<'d>,
{
    type Output = Fb::Output;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let mut state = self.project().state;
        let factory = match state.as_mut().project() {
            StateProj::Pending(factory) => factory.take(),
            StateProj::Active(fiber) => return Fiber::poll(fiber, context),
        };
        let Some(factory) = factory else {
            return Poll::Pending;
        };
        state.set(State::Active(factory()));
        match state.project() {
            StateProj::Active(fiber) => Fiber::poll(fiber, context),
            StateProj::Pending(_) => Poll::Pending,
        }
    }
}
