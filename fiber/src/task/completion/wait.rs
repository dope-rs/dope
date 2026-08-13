use std::{marker, pin, task};

use crate::{abi, context, wait};

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Wait<'target, 'd, F, T> {
    #[pin]
    waiter: wait::Waiter<'target, 'd>,
    f: F,
    marker: marker::PhantomData<fn() -> T>,
}

impl<'target, 'd, F, T> Wait<'target, 'd, F, T>
where
    F: FnMut(
        pin::Pin<&mut context::Context<'_, 'd>>,
        pin::Pin<&wait::Waiter<'target, 'd>>,
    ) -> task::Poll<T>,
{
    pub const fn new(f: F) -> Self {
        use wait::Waiter;
        Self {
            waiter: Waiter::new(),
            f,
            marker: marker::PhantomData,
        }
    }
}

impl<'target, 'd, F, T> abi::Fiber<'d> for Wait<'target, 'd, F, T>
where
    F: FnMut(
        pin::Pin<&mut context::Context<'_, 'd>>,
        pin::Pin<&wait::Waiter<'target, 'd>>,
    ) -> task::Poll<T>,
{
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.project();
        let result = (this.f)(context, this.waiter.as_ref());
        if result.is_ready() {
            this.waiter.as_ref().unregister();
        }
        result
    }
}
