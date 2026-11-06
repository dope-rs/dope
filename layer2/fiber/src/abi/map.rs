use core::task;

use crate::{abi, context};

/// Maps the output of one fiber.
#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Map<F, M> {
    #[pin]
    fiber: F,
    map: M,
}

impl<F, M> Map<F, M> {
    pub const fn new(fiber: F, map: M) -> Self {
        Self { fiber, map }
    }
}

impl<'d, F, M, T> abi::Fiber<'d> for Map<F, M>
where
    F: abi::Fiber<'d>,
    M: FnMut(F::Output) -> T,
{
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<T> {
        let (this, context) = call.into_parts();
        let this = this.project();
        // SAFETY: this owner delegates its admission once; child Ready ends its polling authority.
        unsafe { context::raw::Live::new(this.fiber).delegate(context) }.map(this.map)
    }
}
