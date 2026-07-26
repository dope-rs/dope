use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;

use super::waker::VTABLE;
use crate::{Context, Fiber};
use core::task;

#[repr(transparent)]
pub struct FiberFuture<'d, F> {
    fiber: F,
    marker: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> FiberFuture<'d, F> {
    pub(super) const fn new(fiber: F) -> Self {
        Self {
            fiber,
            marker: PhantomData,
        }
    }
}

impl<'d, F> Future for FiberFuture<'d, F>
where
    F: Fiber<'d>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        debug_assert!(
            core::ptr::eq(cx.waker().vtable(), &VTABLE),
            "fiber polled with a foreign waker"
        );
        let this = unsafe { self.get_unchecked_mut() };
        let mut fiber_cx = unsafe { Context::from_raw_task(cx.waker().data().cast_mut()) };
        let fiber_cx = unsafe { Pin::new_unchecked(&mut fiber_cx) };
        unsafe { Pin::new_unchecked(&mut this.fiber) }.poll(fiber_cx)
    }
}
