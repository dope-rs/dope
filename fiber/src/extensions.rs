use std::io;

use dope::runtime::executor::{self, session};

use crate::abi;

pub trait AppSessionExt<'d> {
    fn block_on<F>(&mut self, fiber: F) -> io::Result<F::Output>
    where
        F: abi::Fiber<'d>;
}

impl<'app, 'd: 'app, D, Q> AppSessionExt<'d> for session::Application<'app, 'd, D, Q>
where
    D: executor::Application<'d>,
{
    fn block_on<F>(&mut self, fiber: F) -> io::Result<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        use crate::task;

        self.drive(task::Once::new(fiber))
    }
}
