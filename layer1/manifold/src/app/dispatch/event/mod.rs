//! Completion capability set.

use std::ops;

use dope_core::{driver::schedule, io};
use dope_runtime::executor;

use crate::app::dispatch;

mod call;
mod driven;
mod set;
mod timed;

pub use call::Call;
pub use driven::Driven;
pub use set::Set;
pub use timed::Timed;

/// A completion consumer that needs no driver authority.
pub trait Complete<'d>: Sized {
    fn dispatch<'app>(
        call: executor::raw::Call<'app, Self, (), io::Event<'d>>,
    ) -> ops::ControlFlow<io::Event<'d>>;
}

/// A completion consumer that also needs driver authority.
pub trait Drive<'d>: Sized {
    type Access;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>;
}

/// Event-only completion capability.
pub struct Observe;

/// A completion consumer that needs scheduler and driver authority.
pub trait Turn<'d>: Sized {
    type Access;

    fn dispatch<'app, 'driver, 'turn>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>;
}
