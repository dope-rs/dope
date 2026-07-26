use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;

use super::Fiber;
use crate::Context;

type Marker<'d, T> = (fn(&'d ()) -> &'d (), fn() -> T);

#[repr(transparent)]
pub struct PollFn<'d, F, T> {
    f: F,
    marker: PhantomData<Marker<'d, T>>,
}

impl<'d, F, T> Unpin for PollFn<'d, F, T> {}

impl<'d, F, T> PollFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>) -> Poll<T>,
{
    pub const fn new(f: F) -> Self {
        Self {
            f,
            marker: PhantomData,
        }
    }
}

impl<'d, F, T> Fiber<'d> for PollFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>) -> Poll<T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        (this.f)(cx)
    }
}
