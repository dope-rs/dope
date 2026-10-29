use std::ops;

use dope_core::io;
use dope_runtime::executor;

use crate::app::dispatch;

/// A completion consumer that also needs driver authority.
pub trait Drive<'d>: Sized {
    type Access;

    fn dispatch<'app, 'driver, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root;
}
