use core::{marker, task};

use crate::{abi, context};

#[must_use = "a fiber does nothing unless it is driven"]
pub struct Pending<T> {
    output: marker::PhantomData<fn() -> T>,
}

impl<T> Pending<T> {
    pub const fn new() -> Self {
        Self {
            output: marker::PhantomData,
        }
    }
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, T> abi::Fiber<'d> for Pending<T> {
    type Output = T;

    fn poll(_call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::Poll;
        Poll::Pending
    }
}
