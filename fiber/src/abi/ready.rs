use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::Context;

#[repr(transparent)]
pub struct Ready<T> {
    output: Option<T>,
}

impl<T> Ready<T> {
    pub(super) const fn new(output: T) -> Self {
        Self {
            output: Some(output),
        }
    }
}

impl<'d, T> Fiber<'d> for Ready<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Poll::Ready(
            unsafe { self.get_unchecked_mut() }
                .output
                .take()
                .expect("fiber polled after completion"),
        )
    }
}
