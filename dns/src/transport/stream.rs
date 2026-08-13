use std::{cell, error, fmt, io, net, time};

use dope::{
    core::{
        driver::{
            self,
            schedule::{self, ready},
        },
        io::socket,
    },
    manifold::{
        self,
        connector::{app, codec, connection, lifecycle, session},
        service::{self, attach, connector, discover, health, observe, reconcile},
        timing,
    },
    net::{
        link::egress::{self, data},
        tcp, wire,
    },
    runtime::random,
};
use o3::cell::region;

use crate::{config, storage::queries, wire::query};

const MAX_MESSAGE: usize = u16::MAX as usize;

type Environment = manifold::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>;
type Connector<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> = connector::Connector<
    'd,
    ID,
    1,
    Session<'d, ID, LANES, ADDRESSES>,
    Endpoints<'d, LANES, ADDRESSES>,
    reconcile::Replace,
    net::SocketAddr,
    observe::Ignore,
    Environment,
    { config::MAX_SERVERS },
>;

#[pin_project::pin_project]
#[derive(dope_gen::Forward)]
pub struct Stream<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> {
    #[pin]
    #[forward]
    inner: Connector<'d, ID, LANES, ADDRESSES>,
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize>
    Stream<'d, ID, LANES, ADDRESSES>
{
    pub(crate) fn new(
        bound: attach::Bound<'d, crate::Storage<LANES, ADDRESSES>>,
        storage: &'d crate::Storage<LANES, ADDRESSES>,
        seed: random::HashState<'d>,
        egress: egress::Config,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        let config = storage.config_ref();
        let inner = connector::Connector::new(
            Session::new(bound),
            Endpoints::new(storage),
            connector::Config::new(
                1,
                health::Backoff::new(config.refresh.backoff.base, seed)?,
                observe::Ignore,
                (),
            )
            .with_egress(egress),
            driver,
        )?;
        Ok(Self { inner })
    }
}

#[doc(hidden)]
pub struct Endpoints<'d, const LANES: usize, const ADDRESSES: usize> {
    storage: &'d crate::Storage<LANES, ADDRESSES>,
    revision: service::Revision,
    active: bool,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointError {
    RevisionExhausted,
    Snapshot(service::ReconcileError),
}

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RevisionExhausted => {
                formatter.write_str("DNS stream endpoint revision is exhausted")
            }
            Self::Snapshot(error) => error.fmt(formatter),
        }
    }
}

impl error::Error for EndpointError {}

impl<'d, const LANES: usize, const ADDRESSES: usize> Endpoints<'d, LANES, ADDRESSES> {
    const fn new(storage: &'d crate::Storage<LANES, ADDRESSES>) -> Self {
        Self {
            storage,
            revision: service::Revision::INITIAL,
            active: false,
        }
    }
}

impl<'d, const LANES: usize, const ADDRESSES: usize>
    discover::Discover<net::SocketAddr, net::SocketAddr, { config::MAX_SERVERS }>
    for Endpoints<'d, LANES, ADDRESSES>
{
    type Metadata = ();
    type Error = EndpointError;

    const REACTIVE: bool = true;

    fn changed(&self) -> bool {
        self.storage.queries.has_stream_work() != self.active
    }

    fn refresh(&mut self, _now: time::Instant, _reason: discover::Refresh) {}

    fn poll(
        &mut self,
        _now: time::Instant,
    ) -> discover::Action<
        net::SocketAddr,
        net::SocketAddr,
        Self::Metadata,
        Self::Error,
        { config::MAX_SERVERS },
    > {
        let desired = self.storage.queries.has_stream_work();
        if desired == self.active {
            return discover::Action::Pending { poll_at: None };
        }
        let Some(revision) = self.revision.next() else {
            return discover::Action::Failed {
                error: EndpointError::RevisionExhausted,
                poll_at: None,
            };
        };
        let snapshot = if desired {
            let config = self.storage.config_ref();
            let endpoints = config
                .servers
                .as_slice()
                .iter()
                .copied()
                .map(|server| service::Endpoint::new(server, server));
            service::Snapshot::try_new(revision, endpoints)
        } else {
            service::Snapshot::try_new(revision, [])
        };
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return discover::Action::Failed {
                    error: EndpointError::Snapshot(error),
                    poll_at: None,
                };
            }
        };
        self.active = desired;
        self.revision = revision;
        discover::Action::Published {
            snapshot,
            metadata: (),
            poll_at: None,
        }
    }
}

#[doc(hidden)]
pub struct Session<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> {
    storage: attach::Bound<'d, crate::Storage<LANES, ADDRESSES>>,
    codec: codec::LengthPrefixed<MAX_MESSAGE>,
    activation: cell::Cell<Option<Activation<'d, ID>>>,
    connection: cell::Cell<Option<Binding<'d, ID>>>,
}

#[derive(Clone, Copy)]
struct Activation<'d, const ID: u8> {
    token: connection::Id<'d, ID>,
    wake: ready::Target<'d>,
}

#[derive(Clone, Copy)]
struct Binding<'d, const ID: u8> {
    token: connection::Id<'d, ID>,
    server: net::SocketAddr,
    wake: ready::Target<'d>,
}

#[doc(hidden)]
#[derive(Default)]
pub struct State {
    close: bool,
}

impl lifecycle::Lifecycle for State {
    fn wants_close(&self) -> lifecycle::Close {
        if self.close {
            lifecycle::Close::Reconnect
        } else {
            lifecycle::Close::Keep
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize>
    Session<'d, ID, LANES, ADDRESSES>
{
    const fn new(storage: attach::Bound<'d, crate::Storage<LANES, ADDRESSES>>) -> Self {
        Self {
            storage,
            codec: codec::LengthPrefixed,
            activation: cell::Cell::new(None),
            connection: cell::Cell::new(None),
        }
    }

    fn binding(&self, token: connection::Id<'d, ID>) -> Option<Binding<'d, ID>> {
        self.connection
            .get()
            .filter(|binding| binding.token == token)
    }

    fn reject_protocol(context: &mut session::Ctx<'_, 'd, Self, ID>) {
        context.close_with(lifecycle::CloseReason::Protocol);
        context.state.close = true;
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> session::Session<'d, ID>
    for Session<'d, ID, LANES, ADDRESSES>
{
    type Codec = codec::LengthPrefixed<MAX_MESSAGE>;
    type ConnState = State;
    type Send = data::Inline<{ query::MAX_STREAM_FRAME_BYTES }>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn activate(&self, token: connection::Id<'d, ID>, wake: ready::Target<'d>) {
        self.activation.set(Some(Activation { token, wake }));
    }

    fn connect(&mut self, peer: socket::Addr, context: &mut session::Ctx<'_, 'd, Self, ID>) {
        let Some(server) = peer.into_std().ok().filter(|server| {
            self.storage
                .config_ref()
                .servers
                .as_slice()
                .contains(server)
        }) else {
            Self::reject_protocol(context);
            return;
        };
        let Some(activation) = self
            .activation
            .get()
            .filter(|activation| activation.token == context.conn_id)
        else {
            Self::reject_protocol(context);
            return;
        };
        self.connection.set(Some(Binding {
            token: context.conn_id,
            server,
            wake: activation.wake,
        }));
        context.state.close = false;
    }

    fn response<'input>(
        &mut self,
        frame: codec::Frame<'d>,
        context: &mut session::Ctx<'_, 'd, Self, ID>,
    ) where
        'd: 'input,
    {
        let Some(binding) = self.binding(context.conn_id) else {
            Self::reject_protocol(context);
            return;
        };
        self.storage
            .queries
            .accept_stream(binding.server, frame.as_ref(), time::Instant::now());
        context.state.close = !self.storage.queries.has_stream_work_for(binding.server);
    }

    fn drain_requests(
        &self,
        token: connection::Id<'d, ID>,
        _parser: &mut <Self::Codec as codec::Codec>::ParseState,
        drain: &mut app::RequestDrain<'_, 'd, Self::Send>,
        region: &mut region::Token<'d>,
    ) -> app::Requests {
        let Some(binding) = self.binding(token) else {
            return app::Requests {
                close: Some(app::CloseKind::Reconnect),
            };
        };
        let now = time::Instant::now();
        loop {
            let (outgoing, permit) =
                match self
                    .storage
                    .queries
                    .admit_stream(now, binding.server, drain)
                {
                    app::RequestAdmission::Item(queries::Start::Prepared(outgoing), permit) => {
                        (outgoing, permit)
                    }
                    app::RequestAdmission::Item(queries::Start::Processed, _) => break,
                    app::RequestAdmission::Empty | app::RequestAdmission::Exhausted => break,
                };

            let frame = match outgoing.with_query(|query| query.encode_stream()) {
                Some(Ok(frame)) => frame,
                Some(Err(_)) | None => break,
            };
            if permit.try_push(region, frame).is_err() {
                break;
            }
            outgoing.commit();
        }
        let rotate = self.storage.queries.has_stream_work()
            && !self.storage.queries.has_stream_work_for(binding.server);
        app::Requests {
            close: rotate.then_some(app::CloseKind::Reconnect),
        }
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> session::Retirement<'d, ID>
    for Session<'d, ID, LANES, ADDRESSES>
{
    fn disconnect(
        &mut self,
        context: &mut session::Ctx<'_, 'd, Self, ID>,
        _reason: lifecycle::CloseReason,
    ) {
        if self
            .connection
            .get()
            .is_some_and(|binding| binding.token == context.conn_id)
        {
            self.connection.set(None);
        }
        if self
            .activation
            .get()
            .is_some_and(|activation| activation.token == context.conn_id)
        {
            self.activation.set(None);
        }
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> session::Scheduling<'d, ID>
    for Session<'d, ID, LANES, ADDRESSES>
{
    fn pre_park(&mut self, work: schedule::Application<'_, 'd>, _region: &mut region::Token<'d>) {
        if self.storage.queries.needs_stream()
            && work.take()
            && let Some(binding) = self.connection.get()
        {
            binding.wake.wake();
        }
    }

    fn progress(&self, _region: &region::Token<'d>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn inbound(
        &self,
        _connection: connection::Id<'d, ID>,
        _state: &Self::ConnState,
        _default: timing::Window,
        _region: &mut region::Token<'d>,
    ) -> app::Inbound {
        timing::Window::new(self.storage.config_ref().query.timeout)
            .map_or(app::Inbound::Quiescent, app::Inbound::Awaiting)
    }
}

impl<'d, const ID: u8, const LANES: usize, const ADDRESSES: usize> session::Target<'d, ID, 1>
    for Session<'d, ID, LANES, ADDRESSES>
{
}
