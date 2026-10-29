use std::ops;

use dope_core::io;
use dope_runtime::executor;

/// A completion consumer that needs no driver authority.
pub trait Complete<'d>: Sized {
    fn dispatch<'app>(
        call: executor::raw::Call<'app, Self, (), io::Event<'d>>,
    ) -> ops::ControlFlow<io::Event<'d>>;
}
