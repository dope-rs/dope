use dope_test::{
    fibers::{Run, Wait},
    harness::signals,
};
pub mod app;
pub mod ports;
mod runtime;

pub use runtime::Runtime;

pub mod case;

use std::{net, time};

use dope_fiber::{BlockOn as _, abi};
use dope_runtime::executor::{self, drive, session};
use dope_test::fibers;

/// Runtime side of a TCP scenario, bound to its application.
pub struct TcpCase<'app, 'life: 'app, 'd: 'life, D: executor::raw::App<'d>> {
    app: session::App<'app, 'life, 'd, D>,
    addr: net::SocketAddr,
}

/// Driver controls shared by topology scenarios without exposing the
/// generation-specific application type to a higher-ranked callback.
pub trait RuntimeCase {
    fn try_wait(&mut self, gate: &dyn signals::Progress, want: u32) -> Result<Wait, Run>;

    fn try_pause(&mut self, duration: time::Duration) -> Result<(), Run>;
}

impl<'app, 'life: 'app, 'd: 'life, D> TcpCase<'app, 'life, 'd, D>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn new(app: session::App<'app, 'life, 'd, D>, addr: net::SocketAddr) -> Self {
        Self { app, addr }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    pub fn try_drive<F: abi::Fiber<'d>>(&mut self, fiber: F) -> Result<F::Output, drive::Error> {
        self.app.block_on(fiber)
    }

    pub fn try_pause(&mut self, duration: time::Duration) -> Result<(), Run> {
        fibers::TEST.try_pause(&mut self.app, duration)
    }

    pub fn try_wait(&mut self, gate: &dyn signals::Progress, want: u32) -> Result<Wait, Run> {
        fibers::TEST.try_wait(&mut self.app, gate, want)
    }
}

impl<'app, 'life: 'app, 'd: 'life, D> RuntimeCase for session::App<'app, 'life, 'd, D>
where
    D: executor::raw::App<'d>,
{
    fn try_wait(&mut self, gate: &dyn signals::Progress, want: u32) -> Result<Wait, Run> {
        fibers::TEST.try_wait(self, gate, want)
    }

    fn try_pause(&mut self, duration: time::Duration) -> Result<(), Run> {
        fibers::TEST.try_pause(self, duration)
    }
}
