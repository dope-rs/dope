use core::pin::Pin;
use core::task::Poll;
use std::process::abort;

use super::Fiber;
use crate::Context;

#[repr(transparent)]
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

impl<'d, T> Fiber<'d> for Ready<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let Some(output) = self.get_mut().output.take() else {
            abort();
        };
        Poll::Ready(output)
    }
}
