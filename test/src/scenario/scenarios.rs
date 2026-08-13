use std::{io, net, time};

use dope::{
    core::driver::{settings, storage},
    manifold::{
        self,
        connector::{
            app,
            attempt::{self, queue},
            connection, session,
        },
        listener::{self, config, handler},
        service::{self, connector, observe, reconcile},
    },
    net::{Transport as _, link::egress, tcp, wire},
    runtime::executor,
};
use o3::collections::slab;

use crate::{checks::Outcome as _, scenario};

type Discovery = service::Fixed<net::SocketAddr, net::SocketAddr, 1>;
type ConnectorEngine<'d, const ID: u8, const MAX: usize, N, E> = connector::Connector<
    'd,
    ID,
    MAX,
    N,
    Discovery,
    reconcile::Preserve,
    net::SocketAddr,
    observe::Ignore,
    E,
    1,
>;
type ConnectorHost<'d, const ID: u8, const MAX: usize, N, E> =
    super::ManifoldHost<'d, ConnectorEngine<'d, ID, MAX, N, E>>;
type ConnectorCase<'app, 'd, const ID: u8, const MAX: usize, N, E> =
    super::TcpCase<'app, 'd, ConnectorHost<'d, ID, MAX, N, E>>;

type AttemptStorage<'d, const ID: u8> = queue::Source<'d, tcp::Tcp, ID>;
type AttemptEngine<'d, const ID: u8, A, E> =
    connection::Engine<'d, ID, A, queue::Control<'d, 'd, tcp::Tcp, ID>, E>;
type AttemptHost<'d, const ID: u8, A, E> = super::ManifoldHost<'d, AttemptEngine<'d, ID, A, E>>;
type AttemptCase<'app, 'd, const ID: u8, A, E> =
    super::TcpCase<'app, 'd, AttemptHost<'d, ID, A, E>>;

struct AttemptFactory<const ID: u8>;

impl<const ID: u8> storage::Factory for AttemptFactory<ID> {
    type Output<'d> = AttemptStorage<'d, ID>;
    type Error = io::Error;

    fn build<'d>(
        self,
        context: &mut storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'d>, Self::Error> {
        let source = queue::Source::with_capacity(slab::Capacity::new(1), context)?;
        Ok(source)
    }
}

pub struct Connector<const MAX_CONNECTIONS: usize> {
    address: net::SocketAddr,
    backoff: time::Duration,
    timer_cache_limit: Option<settings::ScheduleCapacity>,
    egress: egress::Config,
}

impl<const MAX_CONNECTIONS: usize> Connector<MAX_CONNECTIONS> {
    pub const fn new(address: net::SocketAddr, backoff: time::Duration) -> Self {
        Self {
            address,
            backoff,
            timer_cache_limit: None,
            egress: egress::Config::DEFAULT,
        }
    }

    pub const fn timer_cache_limit(mut self, limit: settings::ScheduleCapacity) -> Self {
        self.timer_cache_limit = Some(limit);
        self
    }

    pub const fn egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    pub fn run<const ID: u8, N, E, R>(
        self,
        target: N,
        body: impl for<'app, 'd> FnOnce(&mut ConnectorCase<'app, 'd, ID, MAX_CONNECTIONS, N, E>) -> R,
    ) -> R
    where
        E: manifold::Env<Transport = tcp::Tcp>,
        for<'d> N: session::Target<'d, ID, MAX_CONNECTIONS>,
        for<'d> <E::Wire as wire::Wire>::InitConfig<'d, ID>: Default,
    {
        let mut config = settings::Config::for_tcp_profile::<E::Driver>(MAX_CONNECTIONS)
            .or_abort("connector driver config");
        if let Some(limit) = self.timer_cache_limit {
            let scheduler = config.scheduler().with_timer_cache_limit(limit);
            config = config.with_scheduler(scheduler);
        }
        let executor = executor::Executor::new(config)
            .or_abort("connector executor")
            .with_storage(());
        executor.enter(|mut runtime| {
            use dope::manifold::service::{Endpoint, Revision, Snapshot, health};

            let seed = runtime.hash_state(health::Domain::DEFAULT);
            let backoff = health::Backoff::new(self.backoff, seed).or_abort("connector backoff");
            let discovery = Discovery::new(
                Snapshot::try_new(
                    Revision::new(1),
                    [Endpoint::new(self.address, self.address)],
                )
                .or_abort("single endpoint fits the service snapshot"),
            );
            let connector = ConnectorEngine::<ID, MAX_CONNECTIONS, _, E>::new(
                target,
                discovery,
                connector::Config::new(
                    MAX_CONNECTIONS,
                    backoff,
                    observe::Ignore,
                    Default::default(),
                )
                .with_egress(self.egress),
                &mut runtime.driver_access(),
            )
            .or_abort("service connector");
            runtime
                .with_app(scenario::ManifoldHost::new(connector), |app| {
                    super::TcpCase::invoke(app, self.address, body)
                })
                .or_abort("connector application teardown")
        })
    }
}

/// One exact low-level attempt, used by tests that exercise the raw connector
/// application boundary rather than service discovery or redial policy.
pub struct AttemptConnector {
    address: net::SocketAddr,
    timer_cache_limit: Option<settings::ScheduleCapacity>,
}

impl AttemptConnector {
    pub const fn new(address: net::SocketAddr) -> Self {
        Self {
            address,
            timer_cache_limit: None,
        }
    }

    pub const fn timer_cache_limit(mut self, limit: settings::ScheduleCapacity) -> Self {
        self.timer_cache_limit = Some(limit);
        self
    }

    pub fn run<const ID: u8, A, E, R>(
        self,
        app: A,
        body: impl for<'app, 'd> FnOnce(&mut AttemptCase<'app, 'd, ID, A, E>) -> R,
    ) -> R
    where
        E: manifold::Env<Transport = tcp::Tcp>,
        for<'d> A: app::Application<'d, ID, Wire = E::Wire>
            + app::Receive<'d, ID>
            + app::Lifecycle<'d, ID>
            + app::RequestSource<'d, ID>
            + app::Scheduling<'d, ID>,
        for<'d> <E::Wire as wire::Wire>::InitConfig<'d, ID>: Default,
    {
        let mut config =
            settings::Config::for_tcp_profile::<E::Driver>(1).or_abort("connector driver config");
        if let Some(limit) = self.timer_cache_limit {
            let scheduler = config.scheduler().with_timer_cache_limit(limit);
            config = config.with_scheduler(scheduler);
        }
        executor::Executor::new(config)
            .or_abort("connector executor")
            .with_factory(AttemptFactory::<ID>)
            .try_enter(|mut session| {
                let source = session.storage();
                let attempt = source
                    .dial(attempt::StreamTarget::new(
                        self.address,
                        tcp::Tcp::stream_options(Default::default())
                            .or_abort("default stream options"),
                    ))
                    .or_abort("one-shot attempt capacity");
                let engine = connection::Engine::<ID, _, _, E>::with_attempt_source(
                    app,
                    source,
                    1,
                    egress::Config::default(),
                    Default::default(),
                    &mut session.driver_access(),
                )
                .or_abort("one-shot connector engine");
                let output = session
                    .with_app(scenario::ManifoldHost::new(engine), |app| {
                        super::TcpCase::invoke(app, self.address, body)
                    })
                    .or_abort("connector application teardown");
                drop(attempt);
                output
            })
            .or_abort("one-shot connector storage")
    }
}

type ListenerEngine<'d, const ID: u8, A, E> = listener::Listener<'d, ID, A, E>;
type ListenerHost<'d, const ID: u8, A, E> = super::ManifoldHost<'d, ListenerEngine<'d, ID, A, E>>;
type ListenerCase<'app, 'd, const ID: u8, A, E> =
    super::TcpCase<'app, 'd, ListenerHost<'d, ID, A, E>>;

const DEFAULT_DIRECT_FLIGHTS: usize = 256;

pub struct Listener {
    max_connections: usize,
    direct_flights: Option<usize>,
    egress: egress::Config,
    stream: tcp::StreamConfig,
    transport: tcp::ListenerConfig,
}

impl Listener {
    pub fn new(max_connections: usize, transport: tcp::ListenerConfig) -> Self {
        Self {
            max_connections,
            direct_flights: None,
            egress: egress::Config::DEFAULT,
            stream: Default::default(),
            transport,
        }
    }

    pub fn stream(mut self, stream: tcp::StreamConfig) -> Self {
        self.stream = stream;
        self
    }

    pub fn direct_flights(mut self, direct_flights: usize) -> Self {
        self.direct_flights = Some(direct_flights);
        self
    }

    pub const fn egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    pub fn run<const ID: u8, A, E, R>(
        self,
        app: A,
        body: impl for<'app, 'd> FnOnce(&mut ListenerCase<'app, 'd, ID, A, E>) -> R,
    ) -> R
    where
        E: manifold::Env<Transport = tcp::Tcp>,
        for<'d> A: handler::Application<'d, ID, Wire = E::Wire>,
        for<'d> <E::Wire as wire::Wire>::InitConfig<'d, ID>: Default,
    {
        let config = settings::Config::for_tcp_profile::<E::Driver>(self.max_connections)
            .or_abort("listener driver config");
        let listener_config = config::Config {
            max_connections: self.max_connections,
            direct_flights: self
                .direct_flights
                .unwrap_or(self.max_connections.min(DEFAULT_DIRECT_FLIGHTS)),
            bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
            backlog: 128,
            stream: self.stream,
            transport: self.transport,
            egress: self.egress,
        };
        let executor = executor::Executor::new(config).or_abort("listener executor");
        executor
            .try_enter(|mut session| {
                let hash = session.hash_state(listener::Domain::DEFAULT);
                let listener = listener::Listener::<ID, A, E>::open_in(
                    app,
                    listener_config,
                    hash,
                    &mut session.driver_access(),
                )
                .or_abort("listener open");
                let address = listener.local_addr();
                session
                    .with_app(scenario::ManifoldHost::new(listener), |app| {
                        super::TcpCase::invoke(app, address, body)
                    })
                    .or_abort("listener application teardown")
            })
            .or_abort("listener storage")
    }
}
