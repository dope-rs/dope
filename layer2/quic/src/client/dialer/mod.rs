use crate::runtime::endpoint::control::actions;

use std::net;
use std::ops;
use std::pin;
use std::time;

use o3::buffer::resident;

use dope::core::driver::{route, schedule::clock};
use dope::core::io;
use dope::manifold::app::dispatch::{self, event};
use dope::runtime::executor;

use crate::client;
use crate::client::auth;
use crate::client::build;
use crate::client::life::topology;
use crate::client::protocol;
use crate::conn::scope;
use crate::errors;
use crate::proto::datagram;
use crate::runtime::endpoint::{self, owner};
use crate::runtime::routing;

mod engine;

/// The dialer binds its protocol bridge and receive storage to the same endpoint.
type Endpoint<'pool, 'd, const ID: u8, P, B, A> = endpoint::Endpoint<
    'd,
    ID,
    client::Bridge<'d, ID, P, B>,
    <P as protocol::Protocol<'d>>::Receive,
    endpoint::Idle,
    <A as auth::Mode>::Binding,
    datagram::Sealed<'pool>,
>;

pub(super) enum Connection {
    Current(client::SlotId),
    Attempt(client::AttemptId),
}

/// Pinned client retaining its packet pool, driver, and TLS owners.
#[pin_project::pin_project]
pub struct Dialer<
    'pool,
    'd,
    const ID: u8,
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode = auth::Owned,
    C: client::ConfigSource = client::StaticConfig,
    R: routing::PeerRoute = routing::Plain,
> where
    A::Binding: owner::Binding<'d, ID>,
{
    #[pin]
    pub(super) endpoint: Endpoint<'pool, 'd, ID, P, B, A>,
    pub(super) topology: topology::Topology<A, R>,
    pub(super) config: C,
    pub(super) limits: build::WorkLimits,
}

impl<'pool, 'd, const ID: u8, P, B, A, C, R> dispatch::raw::Projection<'d>
    for Dialer<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
    Endpoint<'pool, 'd, ID, P, B, A>: dispatch::raw::Manifold<'d>,
{
    type Inner = Endpoint<'pool, 'd, ID, P, B, A>;

    fn inner(self: pin::Pin<&Self>) -> pin::Pin<&Self::Inner> {
        self.project_ref().endpoint
    }

    fn inner_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Inner> {
        self.project().endpoint
    }
}

impl<'pool, 'd, const ID: u8, P, B, A, C, R> event::Drive<'d>
    for Dialer<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
    Self: dispatch::raw::Projection<'d>,
    <Self as dispatch::raw::Projection<'d>>::Inner: event::Drive<'d, Access = dispatch::raw::Plain>
        + dispatch::raw::Manifold<
            'd,
            Route = route::KeyTag<ID>,
            PrePark = dispatch::raw::Retained,
            Shutdown = dispatch::raw::Plain,
        >,
{
    type Access = dispatch::raw::Plain;

    fn dispatch<'app, 'driver>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                dispatch::raw::Context<'driver, 'd, Self::Access>,
            ),
        >,
    ) -> ops::ControlFlow<io::Event<'d>> {
        event::Drive::dispatch(call.map_owner(|owner| owner.project().endpoint))
    }
}

impl<'pool, 'd, const ID: u8, P, B, A, C, R> Dialer<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
{
    pub fn protocol(&self) -> &P {
        self.endpoint.handler().protocol()
    }

    /// Returns the first permanent failure, if endpoint admission stopped.
    /// Ordinary packet loss and temporary queue saturation do not set this state.
    pub fn failure(&self) -> Option<protocol::Failure> {
        self.endpoint.handler().failure()
    }

    pub(super) fn protocol_mut(self: pin::Pin<&mut Self>) -> pin::Pin<&mut P> {
        self.project().endpoint.handler_mut().protocol_pin()
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.endpoint.local_addr()
    }

    pub fn len(&self) -> usize {
        self.endpoint.handler().lifecycle().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn smoothed_rtt(&self, slot: client::SlotId) -> Option<time::Duration> {
        let handle = self.endpoint.handler().lifecycle().handle(slot)?;
        self.endpoint.conn(handle)?.recovery().smoothed_rtt()
    }

    pub fn path_stats(&self, slot: client::SlotId) -> Option<client::PathStats> {
        let handle = self.endpoint.handler().lifecycle().handle(slot)?;
        let conn = self.endpoint.conn(handle)?;
        Some(client::PathStats {
            srtt: conn.recovery().smoothed_rtt(),
            min_rtt: conn.recovery().min_rtt(),
            cwnd: conn.recovery().window(),
            bytes_in_flight: conn.recovery().in_flight(),
        })
    }

    pub fn active_len(&self) -> usize {
        self.endpoint.len()
    }

    /// Exact endpoint slots whose current generation completed protocol
    /// establishment. This deliberately differs from physical mux connection
    /// count during handshakes and reconnect teardown.
    pub fn live_len(&self) -> usize {
        self.endpoint.handler().lifecycle().live()
    }

    /// Seals retry admission and requests closure of every retained generation
    /// at one driver-branded observation.
    pub fn close_all(self: pin::Pin<&mut Self>, now: clock::Moment<'d>) {
        now.at(self).apply(|dialer, now| dialer.close_at(now));
    }

    pub fn seal(self: pin::Pin<&mut Self>) {
        self.project().endpoint.handler_mut().life_mut().seal();
    }

    /// Closes the whole logical endpoint set while leaving reconnect
    /// admission open. Multipath applications use this to rotate every path
    /// into one new app generation after any constituent path fails.
    pub(super) fn restart_all(self: pin::Pin<&mut Self>, now: clock::Moment<'d>) {
        now.at(self).apply(|dialer, now| dialer.restart_at(now));
    }

    pub(super) fn try_send(
        self: pin::Pin<&mut Self>,
        slot: client::SlotId,
        data: Vec<u8>,
        now: clock::Moment<'d>,
    ) -> Result<(), errors::SendFailure<Vec<u8>>> {
        now.at(self)
            .apply(|dialer, raw| dialer.try_send_at(slot, data, raw))
    }

    pub(super) fn with_conn<T>(
        self: pin::Pin<&mut Self>,
        target: Connection,
        now: clock::Moment<'d>,
        op: impl for<'scope> FnOnce(
            scope::Temporal<'scope, 'd, ID, P::Receive, resident::Shared<'d>>,
        ) -> T,
    ) -> Result<T, errors::Access> {
        let endpoint = &self.as_ref().get_ref().endpoint;
        let life = endpoint.handler().lifecycle();
        let handle = match target {
            Connection::Current(slot) => life.handle(slot).ok_or(errors::Access::Closed)?,
            Connection::Attempt(attempt) => life.resolve(endpoint.domain().identity(), attempt)?,
        };
        actions::with_connection(self.project().endpoint, handle, now, op)
    }

    pub(super) fn try_packet(
        self: pin::Pin<&mut Self>,
        slot: client::SlotId,
        packet: datagram::Packet<'pool>,
        now: clock::Moment<'d>,
    ) -> Result<(), errors::SendFailure<datagram::Packet<'pool>>> {
        now.at(self)
            .apply(|dialer, raw| dialer.try_pool_at(slot, packet, raw))
    }
}
