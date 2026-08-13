use core::{marker, pin, task};

use crate::{abi, context};

type Marker<'d, T> = (fn(&'d ()) -> &'d (), fn() -> T);

#[repr(transparent)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct PollFn<'d, F, T> {
    f: F,
    marker: marker::PhantomData<Marker<'d, T>>,
}

impl<'d, F, T> Unpin for PollFn<'d, F, T> {}

impl<'d, F, T> PollFn<'d, F, T>
where
    F: FnMut(pin::Pin<&mut context::Context<'_, 'd>>) -> task::Poll<T>,
{
    pub const fn new(f: F) -> Self {
        Self {
            f,
            marker: marker::PhantomData,
        }
    }
}

impl<'d, F, T> abi::Fiber<'d> for PollFn<'d, F, T>
where
    F: FnMut(pin::Pin<&mut context::Context<'_, 'd>>) -> task::Poll<T>,
{
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        (this.f)(cx)
    }
}
