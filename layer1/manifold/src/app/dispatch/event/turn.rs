use std::ops;

use dope_core::{driver::schedule, io};
use dope_runtime::executor;

use crate::app::dispatch;

/// A completion consumer that needs scheduler and driver authority.
pub trait Turn<'d>: Sized {
    type Access;

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root;
}
