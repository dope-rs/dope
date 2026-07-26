use core::future::Future;
use core::marker::PhantomData;

use crate::IntoFiber;
use crate::raw::task::bridges::{FiberFuture, FutureFiber};

pub struct Brand<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

pub struct Seal<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> Brand<'d> {
    /// # Safety
    /// Every awaitable made by the brand is polled only by the paired seal.
    pub const unsafe fn scope() -> (Self, Seal<'d>) {
        (Self(PhantomData), Seal(PhantomData))
    }

    pub fn awaitable<F>(&self, fiber: F) -> FiberFuture<'d, F::IntoFiber>
    where
        F: IntoFiber<'d>,
    {
        FiberFuture::new(fiber.into_fiber())
    }
}

impl<'d> Seal<'d> {
    pub fn future<F>(self, future: F) -> FutureFiber<'d, F>
    where
        F: Future,
    {
        FutureFiber::new(future)
    }
}
