#[doc(hidden)]
pub mod __private;

pub mod batch;
pub mod future;
pub mod pending;
pub mod pollfn;
pub mod race;
pub mod ready;

use core::pin::Pin;
use core::task::Poll;

use super::Context;

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
