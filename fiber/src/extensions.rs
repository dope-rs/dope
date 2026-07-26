use core::pin::Pin;
use std::io;

use dope::driver::token::ROUTE_FRAMEWORK;
use dope::runtime::dispatcher::Dispatcher;
use dope::runtime::executor::{AppSession, Session};
use o3::cell::BrandCell;

use crate::{Fiber, OneShot};

pub trait SessionExt<'d> {
    fn block_on<D, F>(
        &mut self,
        dispatcher: Pin<&BrandCell<'d, D>>,
        fiber: F,
    ) -> io::Result<F::Output>
    where
        D: Dispatcher<'d>,
        F: Fiber<'d>;
}

impl<'scope, 'd: 'scope, S> SessionExt<'d> for Session<'scope, 'd, S> {
    fn block_on<D, F>(
        &mut self,
        dispatcher: Pin<&BrandCell<'d, D>>,
        fiber: F,
    ) -> io::Result<F::Output>
    where
        D: Dispatcher<'d>,
        F: Fiber<'d>,
    {
        self.block_on_with(
            dispatcher,
            OneShot::new(fiber, ROUTE_FRAMEWORK, self.driver())?,
        )
    }
}

pub trait AppSessionExt<'d> {
    fn block_on<F>(&mut self, fiber: F) -> io::Result<F::Output>
    where
        F: Fiber<'d>;
}

impl<'a, 'scope, 'd: 'scope, S, D> AppSessionExt<'d> for AppSession<'a, 'scope, 'd, S, D>
where
    D: Dispatcher<'d>,
{
    fn block_on<F>(&mut self, fiber: F) -> io::Result<F::Output>
    where
        F: Fiber<'d>,
    {
        let driver = self.driver();
        self.block_on_with(OneShot::new(fiber, ROUTE_FRAMEWORK, driver)?)
    }
}
