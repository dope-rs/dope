use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::{Context, Waiter};

pub struct WaitFn<'d, F, T> {
    waiter: Waiter<'d>,
    f: F,
    marker: PhantomData<fn() -> T>,
    _pin: PhantomPinned,
}

impl<'d, F, T> WaitFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>, Pin<&Waiter<'d>>) -> Poll<T>,
{
    pub(super) const fn new(f: F) -> Self {
        Self {
            waiter: Waiter::new(),
            f,
            marker: PhantomData,
            _pin: PhantomPinned,
        }
    }
}

impl<'d, F, T> Fiber<'d> for WaitFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>, Pin<&Waiter<'d>>) -> Poll<T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let waiter = unsafe { Pin::new_unchecked(&this.waiter) };
        let result = (this.f)(cx, waiter);
        if result.is_ready() {
            waiter.unregister();
        }
        result
    }
}
