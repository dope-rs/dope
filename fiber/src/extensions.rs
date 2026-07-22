use core::pin::Pin;
use std::io;

use dope::driver::token::ROUTE_FRAMEWORK;
use dope::runtime::{AppSession, Dispatcher, Session};
use o3::cell::BrandCell;

use crate::{Context, RootWaker, Waker};
use crate::{Fiber, OneShot};
use dope::DriverContext;
use dope_net::link::slot::Slot;
use dope_net::wire::Wire;

pub trait SlotExt<'d> {
    fn waker<'a>(&'a self) -> Waker<'a>;

    fn root_waker(&self) -> RootWaker<'d>;

    fn context<'poll>(&self, driver: DriverContext<'poll, 'd>) -> Context<'poll, 'd>;

    fn park(&self);
}

impl<'d, W: Wire, S> SlotExt<'d> for Slot<'d, W, S> {
    fn waker<'a>(&'a self) -> Waker<'a> {
        Waker::from_ready(self.driver(), self.ready_key()).shorten()
    }

    fn root_waker(&self) -> RootWaker<'d> {
        RootWaker::from_ready(self.driver(), self.ready_key())
    }

    fn context<'poll>(&self, driver: DriverContext<'poll, 'd>) -> Context<'poll, 'd> {
        Context::from_ready(self.driver(), self.ready_key(), driver)
    }

    fn park(&self) {
        self.mark_ready();
    }
}

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
