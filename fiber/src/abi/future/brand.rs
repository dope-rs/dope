use core::marker::PhantomData;

use super::fiber_future::FiberFuture;
use super::seal::Seal;
use crate::IntoFiber;

pub struct Brand<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> Brand<'d> {
    /// # Safety
    /// Brand futures are polled only by the paired seal.
    pub const unsafe fn scope() -> (Self, Seal<'d>) {
        (Self(PhantomData), Seal::new())
    }

    pub fn awaitable<F>(&self, fiber: F) -> FiberFuture<'d, F::IntoFiber>
    where
        F: IntoFiber<'d>,
    {
        FiberFuture::new(fiber.into_fiber())
    }
}
