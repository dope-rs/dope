use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::Context;

pub struct Pending<T> {
    output: PhantomData<fn() -> T>,
}

impl<T> Pending<T> {
    pub(super) const fn new() -> Self {
        Self {
            output: PhantomData,
        }
    }
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, T> Fiber<'d> for Pending<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
