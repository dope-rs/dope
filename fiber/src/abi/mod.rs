pub mod batch;
pub mod future;
mod join;
mod pending;
mod pollfn;
pub mod race;
mod ready;
mod slot;

use core::task;

pub use join::Join;
pub use pending::Pending;
pub use pollfn::PollFn;
pub use ready::Ready;
pub(crate) use slot::Slot;

use crate::context;

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

impl Side {
    const fn other(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[must_use = "a fiber does nothing unless it is driven"]
/// A pinned, driver-scoped unit of cooperative application work.
/// Nested polls share the exact admitted context and its fixed turn budget.
pub trait Fiber<'d>: Sized {
    type Output;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output>;
}

pub trait IntoFiber<'d> {
    type Output;
    type IntoFiber: Fiber<'d, Output = Self::Output>;

    #[must_use = "a fiber does nothing unless it is driven"]
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
