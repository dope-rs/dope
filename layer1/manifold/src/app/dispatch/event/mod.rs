//! Completion capability set.

use std::{ops, pin};

use dope_core::driver::schedule;
use dope_runtime::executor;

use crate::app::dispatch;

mod driven;
mod set;
mod timed;

pub use driven::Driven;
pub use set::Set;
pub use timed::Timed;

/// A completion consumer that needs no driver authority.
pub trait Complete<'d>: Sized {
    fn dispatch<'event>(
        owner: pin::Pin<&mut Self>,
        event: executor::raw::Dispatch<'event, 'd>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>;
}

/// A completion consumer that also needs driver authority.
pub trait Drive<'d>: Sized {
    type Access;

    fn dispatch<'event>(
        owner: pin::Pin<&mut Self>,
        event: executor::raw::Dispatch<'event, 'd>,
        driver: dispatch::raw::Context<'_, 'd, Self::Access>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>;
}

/// Event-only completion capability.
pub struct Observe;

/// A completion consumer that needs scheduler and driver authority.
pub trait Turn<'d>: Sized {
    type Access;

    fn dispatch<'event>(
        owner: pin::Pin<&mut Self>,
        event: executor::raw::Dispatch<'event, 'd>,
        turn: schedule::Turn<'_, 'd>,
        driver: dispatch::raw::Context<'_, 'd, Self::Access>,
    ) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>;
}
