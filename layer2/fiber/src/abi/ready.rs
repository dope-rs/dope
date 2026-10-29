use core::task;

use crate::{abi, context};

#[repr(transparent)]
#[must_use = "a fiber does nothing unless it is driven"]
#[derive(Debug)]
pub struct Ready<T> {
    output: Option<T>,
}

impl<T> Unpin for Ready<T> {}

impl<T> Ready<T> {
    pub const fn new(output: T) -> Self {
        Self {
            output: Some(output),
        }
    }
}

impl<'d, T> abi::Fiber<'d> for Ready<T> {
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::Poll;
        let this = call.into_fiber();
        let Some(output) = this.get_mut().output.take() else {
            return Poll::Pending;
        };
        Poll::Ready(output)
    }
}
