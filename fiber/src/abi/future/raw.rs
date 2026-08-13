use core::{future, marker};

use crate::abi;

pub struct Brand<'d>(marker::PhantomData<fn(&'d ()) -> &'d ()>);

pub struct Seal<'d>(marker::PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> Brand<'d> {
    /// # Safety
    /// Every awaitable made by the brand is polled only by the paired seal.
    pub const unsafe fn scope() -> (Self, Seal<'d>) {
        (Self(marker::PhantomData), Seal(marker::PhantomData))
    }

    pub fn awaitable<F>(&self, fiber: F) -> impl future::Future<Output = F::Output>
    where
        F: abi::IntoFiber<'d>,
    {
        let fiber = abi::IntoFiber::into_fiber(fiber);
        super::Awaitable::new(fiber)
    }
}

impl<'d> Seal<'d> {
    pub fn future<F>(self, future: F) -> impl abi::Fiber<'d, Output = F::Output>
    where
        F: future::Future,
    {
        let Self(marker) = self;
        super::FiberAdapter::new(future, marker)
    }
}
