use core::future::Future;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::task::{Poll, RawWaker, Waker};

use super::waker::VTABLE;
use crate::{Context, Fiber};
use core::task;

#[repr(transparent)]
pub struct FutureFiber<'d, F> {
    future: F,
    marker: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> FutureFiber<'d, F> {
    pub(super) const fn new(future: F) -> Self {
        Self {
            future,
            marker: PhantomData,
        }
    }
}

impl<'d, F> Fiber<'d> for FutureFiber<'d, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let raw = RawWaker::new(cx.as_mut().raw_task().cast_const(), &VTABLE);
        let waker = ManuallyDrop::new(unsafe { Waker::from_raw(raw) });
        let mut future_cx = task::Context::from_waker(&waker);
        unsafe { Pin::new_unchecked(&mut this.future) }.poll(&mut future_cx)
    }
}
