use core::{marker, task};

use o3::base::cell::{local, region};

use crate::{abi, context};

#[must_use = "a fiber does nothing unless it is driven"]
pub struct Once<'d, F, R> {
    call: Option<F>,
    brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    output: marker::PhantomData<fn() -> R>,
}

impl<'d, F, R> Once<'d, F, R>
where
    F: for<'a> FnOnce(local::Access<'a, region::Identity<'d>>) -> R,
{
    pub const fn new(call: F) -> Self {
        Self {
            call: Some(call),
            brand: marker::PhantomData,
            output: marker::PhantomData,
        }
    }
}

impl<F, R> Unpin for Once<'_, F, R> {}

impl<'d, F, R> abi::Fiber<'d> for Once<'d, F, R>
where
    F: for<'a> FnOnce(local::Access<'a, region::Identity<'d>>) -> R,
{
    type Output = R;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let Some(call) = this.call.take() else {
            return task::Poll::Pending;
        };
        let access = local::Access::new(context.as_mut().region_token());
        task::Poll::Ready(call(access))
    }
}
