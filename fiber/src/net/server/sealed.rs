use std::{ops, pin};

use dope::{
    core::{
        driver::{lifecycle, schedule},
        io,
    },
    manifold::{dispatch, dispatch::typed, listener::connection},
    net::{self, wire},
};
use o3::cell::region;

use crate::net::{port, server};

fn apply_queued<'scope, 'd, const ID: u8, T: net::Transport, W: wire::Wire>(
    mut owner: pin::Pin<&mut server::Listener<'scope, 'd, ID, T, W>>,
    target: typed::Token<'d, server::Inner<'scope, 'd, ID, T, W>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
) {
    let port = owner.as_ref().get_ref().port;
    let Some(connection) = owner.as_ref().project_ref().inner.connection_id(target) else {
        return;
    };
    let requests = port.connections.requests(connection);
    apply(owner.as_mut(), target, connection, requests, turn, driver);
}

fn apply<'scope, 'd, const ID: u8, T: net::Transport, W: wire::Wire>(
    mut owner: pin::Pin<&mut server::Listener<'scope, 'd, ID, T, W>>,
    target: typed::Token<'d, server::Inner<'scope, 'd, ID, T, W>>,
    connection: connection::Id<'d, ID>,
    requests: Option<port::Requests<'d>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut dispatch::raw::Context<'_, '_, 'd, dispatch::raw::Retained>,
) {
    let port = owner.as_ref().get_ref().port;
    if let Some(requests) = requests {
        let inner = owner.as_ref().project_ref().inner.get_ref();
        if let Some(bytes) = requests.send
            && !inner.mark_send(driver.region_token(), connection, bytes)
        {
            port.connections.channel().out_of_memory(connection);
        }
        if requests.close {
            inner.close(connection);
        }
    }
    // SAFETY: this owner is reachable only through its installed raw drive methods.
    unsafe {
        dispatch::raw::Manifold::activate(owner.as_mut().project().inner, target, turn, driver)
    };
}

// SAFETY: Listener forwards its complete lifecycle to the pinned inner owner.
unsafe impl<'scope, 'd, const ID: u8, T: net::Transport, W: wire::Wire> dispatch::raw::Manifold<'d>
    for server::Listener<'scope, 'd, ID, T, W>
{
    const ID: u8 = ID;
    type Dispatch = dispatch::raw::Retained;
    type Activate = dispatch::raw::Retained;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        dispatch::raw::Manifold::install(self.project().inner, install);
    }

    unsafe fn dispatch<'turn>(
        self: pin::Pin<&mut Self>,
        ev: io::Event<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<io::Event<'d>> {
        // SAFETY: inherited from this installed owner drive call.
        unsafe { dispatch::raw::Manifold::dispatch(self.project().inner, ev, turn, driver) }
    }

    unsafe fn pre_park<'turn>(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let port = self.as_ref().get_ref().port;
        let work = turn.reborrow().application();
        port.connections
            .maintenance()
            .pre_park(work, driver.region_token());
        while turn.reborrow().maintenance().take() {
            let Some(request) = port.connections.pop_deferred_request() else {
                break;
            };
            let connection = request.token;
            let target = self
                .as_ref()
                .project_ref()
                .inner
                .get_ref()
                .activation_target(connection);
            apply(
                self.as_mut(),
                target,
                connection,
                Some(request.requests),
                turn.reborrow(),
                driver,
            );
        }
        // SAFETY: inherited from this installed owner drive call.
        unsafe { dispatch::raw::Manifold::pre_park(self.project().inner, turn, driver) };
    }

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        target: typed::Token<'d, Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        apply_queued(self, target.retag::<_>(), turn, driver);
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let requests = if self.port.connections.has_deferred_requests() {
            schedule::Progress::Runnable
        } else {
            schedule::Progress::Quiescent
        };
        self.port
            .connections
            .maintenance()
            .progress()
            .reduce(requests)
            .reduce(dispatch::raw::Manifold::progress(
                self.project_ref().inner,
                region,
            ))
    }

    fn shutdown_progress(
        self: pin::Pin<&Self>,
        region: &region::Token<'d>,
    ) -> schedule::Progress<'d> {
        self.port.connections.maintenance().progress().reduce(
            dispatch::raw::Manifold::shutdown_progress(self.project_ref().inner, region),
        )
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        self.port.connections.maintenance().begin_shutdown();
        dispatch::raw::Manifold::shutdown(self.project().inner, turn, driver);
    }

    fn finish(self: pin::Pin<&mut Self>, finish: &mut lifecycle::Finalize<'_, 'd>) {
        dispatch::raw::Manifold::finish(self.project().inner, finish);
    }
}
