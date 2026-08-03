use core::future::Future;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::pin::{Pin, pin};
use core::task::{self, Poll, RawWaker, RawWakerVTable, Waker};
use std::process::abort;

use pin_project::pin_project;

use super::Context;
use crate::abi::Fiber;

static VTABLE: RawWakerVTable =
    RawWakerVTable::new(abort_clone, abort_wake, abort_wake, ignore_drop);

unsafe fn abort_clone(_: *const ()) -> RawWaker {
    abort()
}

unsafe fn abort_wake(_: *const ()) {
    abort()
}

unsafe fn ignore_drop(_: *const ()) {}

#[pin_project]
#[repr(transparent)]
pub(crate) struct FiberFuture<'d, F> {
    #[pin]
    fiber: F,
    marker: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> FiberFuture<'d, F> {
    pub(crate) const fn new(fiber: F) -> Self {
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

    fn poll(self: Pin<&mut Self>, context: &mut task::Context<'_>) -> Poll<Self::Output> {
        debug_assert!(
            core::ptr::eq(context.waker().vtable(), &VTABLE),
            "fiber polled with a foreign waker"
        );

        // SAFETY: a `FiberFuture` can only be created through a brand paired
        // with the `FutureFiber` whose current poll installed this vtable.
        // Its data is that poll's exclusive, pinned task context.
        let parent = unsafe { &mut *context.waker().data().cast_mut().cast::<Context<'_, 'd>>() };
        let mut child = pin!(Context::from_waker(parent.wake, parent.driver.reborrow()));
        Fiber::poll(self.project().fiber, child.as_mut())
    }
}

#[pin_project]
#[repr(transparent)]
pub(crate) struct FutureFiber<'d, F> {
    #[pin]
    future: F,
    marker: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> FutureFiber<'d, F> {
    pub(crate) const fn new(future: F) -> Self {
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

    fn poll(self: Pin<&mut Self>, context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let task = context.as_ref().get_ref() as *const Context<'_, 'd> as *const ();
        let raw = RawWaker::new(task, &VTABLE);
        // SAFETY: the vtable never releases the borrowed data. Clone and wake
        // abort, and `ManuallyDrop` prevents the no-op drop callback, so this
        // waker cannot escape the synchronous future poll below.
        let waker = ManuallyDrop::new(unsafe { Waker::from_raw(raw) });
        let mut future_context = task::Context::from_waker(&waker);
        Future::poll(self.project().future, &mut future_context)
    }
}
