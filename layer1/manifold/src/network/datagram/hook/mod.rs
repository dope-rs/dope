use std::{convert, marker, pin};

use dope_core::driver::{
    self,
    life::finish,
    route::Identity,
    schedule::{self, clock},
};
use dope_runtime::executor;
use o3::{
    base::cell::region,
    buffer::{self, pool},
};

use crate::{
    app::dispatch,
    network::datagram::{self, send},
};

pub mod app;
pub mod ingress;
pub mod raw;
mod transit;

pub use transit::Transit;

pub struct Enabled;
pub struct Disabled;

pub struct Set<
    I = Disabled,
    S = Disabled,
    F = Disabled,
    D = Disabled,
    R = Disabled,
    B = pool::Cursor,
    M = send::Plain,
>(marker::PhantomData<(I, S, F, D, R, B, M)>);

pub trait Bundle {
    type Buffer: buffer::Residency + buffer::raw::Source + Sized;
    type Send: send::Mode;
    type Install;
    type Shutdown;
    type Finalize;
    type Drive;
    type Settlement;
}

/// Socket storage selected by one hook bundle.
pub type Socket<'d, ID, H> = datagram::Socket<'d, ID, <H as Bundle>::Buffer, <H as Bundle>::Send>;

type Endpoint<'d, ID, H> = datagram::Endpoint<
    'd,
    ID,
    H,
    <<H as datagram::Ingress<'d, ID>>::Hooks as Bundle>::Buffer,
    <<H as datagram::Ingress<'d, ID>>::Hooks as Bundle>::Send,
>;

impl<I, S, F, D, R, B: buffer::Residency + buffer::raw::Source, M: send::Mode> Bundle
    for Set<I, S, F, D, R, B, M>
{
    type Buffer = B;
    type Send = M;
    type Install = I;
    type Shutdown = S;
    type Finalize = F;
    type Drive = D;
    type Settlement = R;
}

pub trait Shutdown<'d, ID: Identity, H, R>
where
    H: datagram::Ingress<'d, ID>,
{
    fn shutdown<'a, 'turn>(
        endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>>;
}

pub trait Finalize<'d, ID: Identity, H, R>
where
    H: datagram::Ingress<'d, ID>,
{
    fn finalize<'a>(
        endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>>;
}

pub trait Schedule<'d, ID: Identity, H>
where
    H: datagram::Ingress<'d, ID>,
{
    type Plan<'turn>;

    fn select<'turn>(clock: clock::Clock<'turn, 'd>) -> Option<Self::Plan<'turn>>;

    fn progress<'turn>(
        endpoint: pin::Pin<&Endpoint<'d, ID, H>>,
        plan: Self::Plan<'turn>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d>;
}

impl<'d, ID: Identity, H> dispatch::raw::Installs<'d, Endpoint<'d, ID, H>> for Disabled
where
    H: datagram::Ingress<'d, ID>,
{
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, Endpoint<'d, ID, H>>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Endpoint<'d, ID, H>> {
        install
    }
}

impl<'d, ID: Identity, H> dispatch::raw::Installs<'d, Endpoint<'d, ID, H>> for Enabled
where
    H: datagram::Ingress<'d, ID> + datagram::Install,
{
    fn call<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, Endpoint<'d, ID, H>>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Endpoint<'d, ID, H>> {
        let (mut endpoint, proof) = install.into_parts();
        let this = endpoint.as_mut().project();
        this.handler.installed();
        executor::raw::Install::new(endpoint, proof)
    }
}

impl<'d, ID: Identity, H> Shutdown<'d, ID, H, Disabled> for Disabled
where
    H: datagram::Ingress<'d, ID>,
{
    fn shutdown<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        this.socket.as_mut().stop(driver);
        this.socket.drain(turn.maint());
        endpoint
    }
}

impl<'d, ID: Identity, H> Shutdown<'d, ID, H, Enabled> for Disabled
where
    H: datagram::Settlement<'d, ID>,
{
    fn shutdown<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        let now = driver.driver_ref().clock().now();
        this.socket.as_mut().stop(driver);
        this.socket
            .drain_to(driver.state_token(), turn.maint(), now, this.handler);
        endpoint
    }
}

impl<'d, ID: Identity, H> Shutdown<'d, ID, H, Disabled> for Enabled
where
    H: datagram::Ingress<'d, ID> + datagram::Shutdown,
{
    fn shutdown<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        this.socket.as_mut().stop(driver);
        this.handler.as_mut().shutdown();
        this.socket.drain(turn.maint());
        endpoint
    }
}

impl<'d, ID: Identity, H> Shutdown<'d, ID, H, Enabled> for Enabled
where
    H: datagram::Settlement<'d, ID> + datagram::Shutdown,
{
    fn shutdown<'a, 'turn>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        let now = driver.driver_ref().clock().now();
        this.socket.as_mut().stop(driver);
        this.handler.as_mut().shutdown();
        this.socket
            .drain_to(driver.state_token(), turn.maint(), now, this.handler);
        endpoint
    }
}

impl<'d, ID: Identity, H> Finalize<'d, ID, H, Disabled> for Disabled
where
    H: datagram::Ingress<'d, ID>,
{
    fn finalize<'a>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        endpoint.as_mut().project().socket.finish(finish);
        endpoint
    }
}

impl<'d, ID: Identity, H> Finalize<'d, ID, H, Enabled> for Disabled
where
    H: datagram::Settlement<'d, ID>,
{
    fn finalize<'a>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let this = endpoint.as_mut().project();
        this.socket.finish_to(this.handler, finish);
        endpoint
    }
}

impl<'d, ID: Identity, H> Finalize<'d, ID, H, Disabled> for Enabled
where
    H: datagram::Ingress<'d, ID> + datagram::Finalize,
{
    fn finalize<'a>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        this.socket.as_mut().finish(finish);
        let state = this.socket.as_ref().get_ref().receive.residency();
        this.handler.finalized(state);
        endpoint
    }
}

impl<'d, ID: Identity, H> Finalize<'d, ID, H, Enabled> for Enabled
where
    H: datagram::Settlement<'d, ID> + datagram::Finalize,
{
    fn finalize<'a>(
        mut endpoint: pin::Pin<&'a mut Endpoint<'d, ID, H>>,
        finish: &mut finish::Finish<'_, 'd>,
    ) -> pin::Pin<&'a mut Endpoint<'d, ID, H>> {
        let mut this = endpoint.as_mut().project();
        this.socket
            .as_mut()
            .finish_to(this.handler.as_mut(), finish);
        let state = this.socket.as_ref().get_ref().receive.residency();
        this.handler.finalized(state);
        endpoint
    }
}

impl<'d, ID: Identity, H> Schedule<'d, ID, H> for Disabled
where
    H: datagram::Ingress<'d, ID>,
{
    type Plan<'turn> = convert::Infallible;

    fn select<'turn>(_: clock::Clock<'turn, 'd>) -> Option<Self::Plan<'turn>> {
        None
    }

    fn progress<'turn>(
        _: pin::Pin<&Endpoint<'d, ID, H>>,
        plan: Self::Plan<'turn>,
        _: &region::Token<region::Identity<'d, 1>>,
    ) -> schedule::Progress<'d> {
        match plan {}
    }
}

const _: () = {
    assert!(std::mem::size_of::<Set>() == 0);
    assert!(!std::mem::needs_drop::<Set>());
    assert!(std::mem::size_of::<Set<Enabled, Enabled, Enabled, Enabled, Enabled>>() == 0);
    assert!(!std::mem::needs_drop::<
        Set<Enabled, Enabled, Enabled, Enabled, Enabled>,
    >());
};
