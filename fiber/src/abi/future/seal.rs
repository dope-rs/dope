use core::future::Future;
use core::marker::PhantomData;

use super::future_fiber::FutureFiber;

pub struct Seal<'d>(PhantomData<fn(&'d ()) -> &'d ()>);

impl<'d> Seal<'d> {
    pub(super) const fn new() -> Self {
        Self(PhantomData)
    }

    pub fn future<F>(self, future: F) -> FutureFiber<'d, F>
    where
        F: Future,
    {
        FutureFiber::new(future)
    }
}
