use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use pin_project::pin_project;

use crate::abi::Fiber;
use crate::raw::task::Context;
use crate::raw::wait::Waiter;

#[pin_project]
pub struct WaitFn<'d, F, T> {
    #[pin]
    waiter: Waiter<'d>,
    f: F,
    marker: PhantomData<fn() -> T>,
}

impl<'d, F, T> WaitFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>, Pin<&Waiter<'d>>) -> Poll<T>,
{
    pub const fn new(f: F) -> Self {
        Self {
            waiter: Waiter::new(),
            f,
            marker: PhantomData,
        }
    }
}

impl<'d, F, T> Fiber<'d> for WaitFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>, Pin<&Waiter<'d>>) -> Poll<T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        let result = (this.f)(context, this.waiter.as_ref());
        if result.is_ready() {
            this.waiter.as_ref().unregister();
        }
        result
    }
}
