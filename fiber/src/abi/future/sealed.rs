use core::{future, marker, pin, task};
use std::process;

use crate::{abi, context};

static VTABLE: task::RawWakerVTable =
    task::RawWakerVTable::new(abort_clone, abort_wake, abort_wake, ignore_drop);

unsafe fn abort_clone(_: *const ()) -> task::RawWaker {
    process::abort()
}

unsafe fn abort_wake(_: *const ()) {
    process::abort()
}

unsafe fn ignore_drop(_: *const ()) {}

#[pin_project::pin_project]
#[repr(transparent)]
pub(crate) struct Awaitable<'d, F> {
    #[pin]
    fiber: F,
    marker: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> Awaitable<'d, F> {
    pub(crate) const fn new(fiber: F) -> Self {
        Self {
            fiber,
            marker: marker::PhantomData,
        }
    }
}

impl<'d, F> future::Future for Awaitable<'d, F>
where
    F: abi::Fiber<'d>,
{
    type Output = F::Output;

    fn poll(
        self: pin::Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        debug_assert!(
            core::ptr::eq(context.waker().vtable(), &VTABLE),
            "fiber polled with a foreign waker"
        );

        // SAFETY: an `Awaitable` can only be created through a brand paired
        // with the fiber adapter whose current poll installed this vtable.
        // Its data is that poll's exclusive, pinned task context.
        let parent = unsafe {
            &mut *context
                .waker()
                .data()
                .cast_mut()
                .cast::<context::Context<'_, 'd>>()
        };
        // SAFETY: the adapter's outer poll receives this context pinned for
        // the entire future poll, as required by the Fiber ABI.
        let mut parent = unsafe { pin::Pin::new_unchecked(parent) };
        parent
            .as_mut()
            .try_poll(self.project().fiber)
            .unwrap_or(task::Poll::Pending)
    }
}

#[pin_project::pin_project]
#[repr(transparent)]
pub(crate) struct FiberAdapter<'d, F> {
    #[pin]
    future: F,
    marker: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F> FiberAdapter<'d, F> {
    pub(crate) const fn new(future: F, marker: marker::PhantomData<fn(&'d ()) -> &'d ()>) -> Self {
        Self { future, marker }
    }
}

impl<'d, F> abi::Fiber<'d> for FiberAdapter<'d, F>
where
    F: future::Future,
{
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::{mem::ManuallyDrop, task::RawWaker};

        let (this, context) = call.into_parts();

        // SAFETY: the exclusive pinned context supplies the pointer provenance
        // and is not used again after this reborrow.
        let task = unsafe { pin::Pin::get_unchecked_mut(context) } as *mut context::Context<'_, 'd>
            as *const ();
        let raw = RawWaker::new(task, &VTABLE);
        // SAFETY: the vtable never releases the borrowed data. Clone and wake
        // abort, and `ManuallyDrop` prevents the no-op drop callback, so this
        // waker cannot escape the synchronous future poll below.
        let waker = ManuallyDrop::new(unsafe {
            use core::task::Waker;
            Waker::from_raw(raw)
        });
        let mut future_context = task::Context::from_waker(&waker);
        future::Future::poll(this.project().future, &mut future_context)
    }
}
