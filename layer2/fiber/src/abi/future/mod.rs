use core::{future, marker, mem, pin, ptr, task};

use crate::{abi, context};

/// Affine brand and seal capabilities used by generated fiber adapters.
pub mod raw;

static BORROWED_VTABLE: task::RawWakerVTable =
    task::RawWakerVTable::new(clone_inert, noop, noop, noop);
/// # Safety
/// `data` borrows one live adapter poll frame; this callback neither owns nor dereferences it.
/// Cloning preserves the identical `(data, vtable)` pair required for interchangeable wakers.
unsafe fn clone_inert(data: *const ()) -> task::RawWaker {
    task::RawWaker::new(data, &BORROWED_VTABLE)
}

unsafe fn noop(_: *const ()) {}

#[pin_project::pin_project]
#[repr(transparent)]
#[doc(hidden)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Awaitable<'a, 'd, I, F> {
    #[pin]
    fiber: F,
    marker: marker::PhantomData<&'a mut self::raw::Brand<'d, I>>,
}

impl<'a, 'd, I, F> future::Future for Awaitable<'a, 'd, I, F>
where
    F: abi::Fiber<'d>,
{
    type Output = F::Output;

    fn poll(
        self: pin::Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        // SAFETY: the affine brand admits this awaitable only inside its paired
        // adapter poll, whose waker data is the live frame with the same scope.
        let frame = unsafe {
            &mut *context
                .waker()
                .data()
                .cast_mut()
                .cast::<self::raw::Frame<'_, '_, 'd, I>>()
        };
        frame.poll(self.project().fiber)
    }
}

#[pin_project::pin_project]
#[repr(transparent)]
#[doc(hidden)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct FiberAdapter<'d, I, F> {
    #[pin]
    future: F,
    seal: self::raw::Seal<'d, I>,
}

impl<'d, I, F> abi::Fiber<'d> for FiberAdapter<'d, I, F>
where
    F: future::Future,
{
    type Output = F::Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::{RawWaker, Waker};

        let (this, context) = call.into_parts();

        let mut frame = self::raw::Frame::<I>::new(context);
        let data = ptr::from_mut(&mut frame).cast_const().cast();
        let raw = RawWaker::new(data, &BORROWED_VTABLE);
        let waker = mem::ManuallyDrop::new(unsafe { Waker::from_raw(raw) });
        let mut cx = task::Context::from_waker(&waker);
        future::Future::poll(this.project().future, &mut cx)
    }
}
