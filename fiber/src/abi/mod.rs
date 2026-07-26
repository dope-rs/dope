#[doc(hidden)]
pub mod __private;

pub mod batch;
mod future;
pub mod pending;
pub mod pollfn;
pub mod ready;
mod scoped;

use core::pin::Pin;
use core::task::Poll;

use super::Context;
use pending::Pending;
use pollfn::PollFn;
use ready::Ready;

pub const fn pending<T>() -> Pending<T> {
    Pending::new()
}

pub const fn poll_fn<'d, F, T>(f: F) -> PollFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>) -> Poll<T>,
{
    PollFn::new(f)
}

pub const fn ready<T>(output: T) -> Ready<T> {
    Ready::new(output)
}

pub trait Fiber<'d> {
    type Output;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output>;
}

pub trait IntoFiber<'d> {
    type Output;
    type IntoFiber: Fiber<'d, Output = Self::Output>;

    fn into_fiber(self) -> Self::IntoFiber;
}

impl<'d, F> IntoFiber<'d> for F
where
    F: Fiber<'d>,
{
    type Output = F::Output;
    type IntoFiber = Self;

    fn into_fiber(self) -> Self::IntoFiber {
        self
    }
}
