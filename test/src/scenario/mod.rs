pub mod rt;
pub mod scenarios;
mod sealed;

use std::{cell, net, pin, thread, time};

use dope::{
    manifold::dispatch,
    runtime::executor::{self, session},
};
use dope_fiber::{abi, extensions::AppSessionExt as _};
pub(crate) use sealed::Scope;

use crate::{checks::Outcome as _, fibers, peer};

/// Generic single-manifold dispatcher used by integration scenarios.
#[pin_project::pin_project]
#[derive(dope_gen::Application)]
pub struct ManifoldHost<'d, M>
where
    M: dispatch::raw::Manifold<'d>,
{
    #[pin]
    #[manifold]
    manifold: M,
    #[dispatcher(marker)]
    driver: ::core::marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, M> ManifoldHost<'d, M>
where
    M: dispatch::raw::Manifold<'d>,
{
    pub fn new(manifold: M) -> Self {
        Self {
            manifold,
            driver: ::core::marker::PhantomData,
        }
    }

    /// Selects the pinned manifold for runtime client issuance.
    pub fn manifold(self: pin::Pin<&Self>) -> pin::Pin<&M> {
        self.project_ref().manifold
    }
}

/// Runtime side of a TCP scenario. It owns no resources and cannot escape `Executor::enter`.
pub struct TcpCase<'app, 'd: 'app, D> {
    app: session::Application<'app, 'd, D>,
    addr: net::SocketAddr,
    operations: cell::Cell<usize>,
}

impl<'app, 'd: 'app, D> TcpCase<'app, 'd, D>
where
    D: executor::Application<'d>,
{
    pub fn invoke<R>(
        app: session::Application<'app, 'd, D>,
        address: net::SocketAddr,
        body: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let mut case = Self::new(app, address);
        body(&mut case)
    }

    pub fn new(app: session::Application<'app, 'd, D>, addr: net::SocketAddr) -> Self {
        Self {
            app,
            addr,
            operations: cell::Cell::new(0),
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    pub fn peer<T: Send + 'static>(
        &self,
        script: impl FnOnce(&mut net::TcpStream) -> T + Send + 'static,
    ) -> thread::JoinHandle<T> {
        self.operations.set(self.operations.get() + 1);
        peer::Peer::at(self.addr).spawn(script)
    }

    pub fn request_reply(&self, request: Vec<u8>) -> thread::JoinHandle<Vec<u8>> {
        self.operations.set(self.operations.get() + 1);
        peer::Peer::at(self.addr).request_reply(request)
    }

    pub fn drive<F: abi::Fiber<'d>>(&mut self, fiber: F) -> F::Output {
        self.operations.set(self.operations.get() + 1);
        self.app.block_on(fiber).or_abort("drive scenario fiber")
    }

    pub fn until(&mut self, gate: &fibers::Gate, want: u32) {
        self.operations.set(self.operations.get() + 1);
        fibers::TEST.run_until(&mut self.app, gate, want);
    }

    pub fn wait_until(&mut self, gate: &fibers::Gate, want: u32) -> bool {
        self.operations.set(self.operations.get() + 1);
        fibers::TEST.wait_until(&mut self.app, gate, want)
    }

    pub fn pause(&mut self, duration: time::Duration) {
        self.operations.set(self.operations.get() + 1);
        fibers::TEST.pause(&mut self.app, duration);
    }
}
