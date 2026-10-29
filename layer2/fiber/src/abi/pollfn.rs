use core::{marker, pin, task};

use crate::{abi, context};

#[repr(transparent)]
#[must_use = "a fiber does nothing unless it is driven"]
#[derive(Debug)]
pub struct PollFn<'d, F> {
    f: F,
    driver: marker::PhantomData<&'d mut &'d ()>,
}

impl<'d, F> Unpin for PollFn<'d, F> {}

pub const fn poll_fn<'d, F, T>(mut f: F) -> impl abi::Fiber<'d, Output = T>
where
    F: FnMut(pin::Pin<&mut context::Context<'_, 'd>>) -> task::Poll<T>,
{
    PollFn::admitted(move |admit| f(admit.into_context()))
}

impl<'d, F> PollFn<'d, F> {
    /// Retains the poll's initial admission for one child task or ready group.
    pub const fn admitted<T>(f: F) -> Self
    where
        F: FnMut(context::Admission<'_, '_, 'd>) -> task::Poll<T>,
    {
        Self {
            f,
            driver: marker::PhantomData,
        }
    }
}

impl<'d, F, T> abi::Fiber<'d> for PollFn<'d, F>
where
    F: FnMut(context::Admission<'_, '_, 'd>) -> task::Poll<T>,
{
    type Output = T;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let this = this.get_mut();
        (this.f)(admit)
    }
}
