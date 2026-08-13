use std::{ops, pin};

use dope_core::driver::{lifecycle, schedule};
use o3::cell::region;

use crate::{
    connector::{auxiliary, session},
    dispatch::typed,
    service::{connector, discover, observe, reconcile},
};

// SAFETY: Connector delegates every retained lifecycle to its pinned engine.
unsafe impl<
    'd,
    const ID: u8,
    const MAX_CONNECTIONS: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    D,
    O,
    X,
> crate::dispatch::raw::Manifold<'d>
    for connector::Connector<'d, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, connector::Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, connector::Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
    X: auxiliary::Mode<'d, N::Send, ID>,
{
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Retained;
    type Activate = crate::dispatch::raw::Retained;
    type PrePark = crate::dispatch::raw::Retained;
    type Shutdown = crate::dispatch::raw::Retained;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        crate::dispatch::raw::Manifold::install(self.project().engine, install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        event: crate::DriverEvent<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<crate::DriverEvent<'d>> {
        // SAFETY: inherited from this forwarding method's installed-owner contract.
        unsafe {
            crate::dispatch::raw::Manifold::dispatch(self.project().engine, event, turn, driver)
        }
    }

    unsafe fn pre_park<'turn>(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let now = driver.turn_now();
        if self.as_ref().discovery_due(now)
            && let Some(permit) =
                schedule::MaintenancePermit::try_take(turn.reborrow().maintenance())
        {
            self.as_mut().poll_discovery(now, permit);
        }
        // SAFETY: inherited from this forwarding method's installed-owner contract.
        unsafe {
            crate::dispatch::raw::Manifold::pre_park(
                self.as_mut().project().engine,
                turn.reborrow(),
                driver,
            )
        };
        if <R as reconcile::Supported>::AUTHORITATIVE {
            self.apply_retirements(turn);
        }
    }

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: typed::Token<'d, Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let target = target.retag::<_>();
        // SAFETY: inherited from this forwarding method's installed-owner contract.
        unsafe {
            crate::dispatch::raw::Manifold::activate(self.project().engine, target, turn, driver)
        };
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        if <R as reconcile::Supported>::AUTHORITATIVE && self.as_ref().has_retirements() {
            return schedule::Progress::Runnable;
        }
        let this = self.project_ref();
        let discovery = match *this.poll_at {
            Some(deadline) => schedule::Progress::until(region, deadline),
            None => schedule::Progress::Quiescent,
        };
        crate::dispatch::raw::Manifold::progress(this.engine, region).reduce(discovery)
    }

    fn shutdown_progress(
        self: pin::Pin<&Self>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        crate::dispatch::raw::Manifold::shutdown_progress(self.project_ref().engine, region)
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        crate::dispatch::raw::Manifold::shutdown(self.project().engine, turn, driver);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        crate::dispatch::raw::Manifold::finish(self.project().engine, finish);
    }
}
