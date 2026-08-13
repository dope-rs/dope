use std::ops;

use crate::{
    driver::{ops::poll, schedule},
    io,
};

pub trait Source<'d>: poll::Poll<'d> {
    /// Drives directly from the backend queue, materializing one event at a time.
    #[doc(hidden)]
    fn dispatch(
        &mut self,
        work: schedule::Reactor<'_, 'd>,
        dispatch: impl FnMut(io::Event<'d>, &mut Self) -> ops::ControlFlow<io::Event<'d>>,
    ) -> poll::Dispatch<'d>;
}
