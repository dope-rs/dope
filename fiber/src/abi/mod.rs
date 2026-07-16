#[doc(hidden)]
pub mod __private;

mod batch;
mod future;
mod pending;
mod pollfn;
mod ready;
mod scoped;
mod waitfn;

use core::pin::Pin;
use core::task::Poll;

use super::{Context, Waiter};
pub use batch::{Batch, BatchOutput};
pub use future::lazy::Lazy;
pub use pending::Pending;
pub use pollfn::PollFn;
pub use ready::Ready;
pub use waitfn::WaitFn;

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

pub const fn wait_fn<'d, F, T>(f: F) -> WaitFn<'d, F, T>
where
    F: FnMut(Pin<&mut Context<'_, 'd>>, Pin<&Waiter<'d>>) -> Poll<T>,
{
    WaitFn::new(f)
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
