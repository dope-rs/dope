use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::thread::JoinHandle;

use dope::driver::token::Token;
use dope::manifold::Manifold;
use dope::manifold::typed::TypedToken;
use dope::runtime::dispatcher::{Dispatcher, Idle};
use dope::runtime::executor::Session;
use dope::{DriverContext, Event};
use dope_fiber::abi::Fiber;
use o3::cell::BrandCell;

use crate::{Gate, drive, request_reply, run_until, spawn_peer};
use dope::manifold::listener::Listener;
use pin_project::pin_project;
use std::rc::Rc;

/// Generic single-manifold dispatcher used by integration scenarios.
#[pin_project]
pub struct ManifoldHost<M> {
    #[pin]
    manifold: M,
}

impl<M> ManifoldHost<M> {
    pub fn new(manifold: M) -> Self {
        Self { manifold }
    }
}

impl<'d, M> Dispatcher<'d> for ManifoldHost<M>
where
    M: Manifold<'d>,
{
    fn dispatch(self: Pin<&mut Self>, event: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        if event.route() == M::ID {
            Manifold::dispatch(self.project().manifold, event, driver);
        }
    }

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        if target.route() == M::ID {
            let target = unsafe { TypedToken::new_unchecked(target) };
            Manifold::activate(self.project().manifold, target, driver);
        }
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::pre_park(self.project().manifold, driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Manifold::idle(self.project_ref().manifold)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::shutdown(self.project().manifold, driver);
    }
}

pub type ListenerHost<'d, const ID: u8, A, E> = ManifoldHost<Listener<'d, ID, A, E>>;

/// Runtime side of a TCP scenario. It owns no resources and cannot escape `Executor::enter`.
pub struct TcpCase<'a, 'scope, 'd, D> {
    session: &'a mut Session<'scope, 'd>,
    app: Pin<&'a BrandCell<'d, D>>,
    addr: SocketAddr,
}

impl<'a, 'scope, 'd, D> TcpCase<'a, 'scope, 'd, D>
where
    D: Dispatcher<'d>,
{
    pub fn new(
        session: &'a mut Session<'scope, 'd>,
        app: Pin<&'a BrandCell<'d, D>>,
        addr: SocketAddr,
    ) -> Self {
        Self { session, app, addr }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn session(&mut self) -> &mut Session<'scope, 'd> {
        self.session
    }

    pub fn peer<T: Send + 'static>(
        &self,
        script: impl FnOnce(&mut TcpStream) -> T + Send + 'static,
    ) -> JoinHandle<T> {
        spawn_peer(self.addr, script)
    }

    pub fn request_reply(&self, request: Vec<u8>) -> JoinHandle<Vec<u8>> {
        request_reply(self.addr, request)
    }

    pub fn drive<F: Fiber<'d>>(&mut self, fiber: F) -> F::Output {
        drive(self.session, self.app.as_ref(), fiber)
    }

    pub fn until(&mut self, gate: &Rc<Gate>, want: u32) {
        run_until(self.session, self.app.as_ref(), gate, want);
    }
}
