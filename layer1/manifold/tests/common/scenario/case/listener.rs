use std::{convert::Infallible, marker, net, time::Duration};

use dope_core::{
    driver::{
        life::recovery,
        route::{Identity, table::conn},
        settings::{self, errors},
    },
    io::socket,
};
use dope_manifold::{
    app::{self, env, env::quota, receive},
    network::listener::{self, config, handler, topology::Direct},
};
use dope_net::{
    link::egress,
    tcp,
    wire::{self, domain},
};
use dope_runtime::{
    executor::{self, session, session::Completion},
    topology::{self, Plan, error::Kind, lending, ops},
};

use crate::common::scenario::{
    TcpCase,
    case::{Failure, Value},
};

const DEFAULT_DIRECT_FLIGHTS: usize = 256;

pub(crate) struct Listen<'report, F>(F, &'report mut Completion);

impl<
    'report,
    'n,
    'a,
    's,
    'stage,
    'scope,
    'life: 'scope,
    'p: 'life,
    'd: 'p,
    ID: Identity,
    A,
    E,
    F,
    R,
>
    lending::Next<
        lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>,
        app::Host<listener::Listener<'n, 'a, 's, 'd, 'd, ID, A, E>>,
    > for Listen<'report, F>
where
    E: env::Spec<Wire = A::Wire>,
    A: handler::Application<'d, ID> + handler::Runtime<'d, 'd, ID>,
    app::Host<listener::Listener<'n, 'a, 's, 'd, 'd, ID, A, E>>: executor::raw::App<'d>,
    F: for<'app> FnOnce(
        &mut TcpCase<'app, 'life, 'd, app::Host<listener::Listener<'n, 'a, 's, 'd, 'd, ID, A, E>>>,
    ) -> R,
{
    type Output = Result<R, session::Error<'report, R>>;

    fn run(
        self,
        host: app::Host<listener::Listener<'n, 'a, 's, 'd, 'd, ID, A, E>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>,
    ) -> Self::Output {
        stage.with_quiesce(self.1, host, Duration::from_secs(2), |mut app| {
            let addr = app.inspect(|host| host.manifold(), |server| server.local_addr());
            let mut case = TcpCase::new(app, addr);
            (self.0)(&mut case)
        })
    }
}

pub struct Listener<I = receive::Borrowed> {
    per_ip: Option<quota::PerIp>,
    connections: conn::Capacity,
    direct_flights: Option<config::Flights>,
    egress: egress::Config,
    stream: tcp::StreamConfig,
    transport: tcp::Config,
    connection_lifetime: Option<handler::Lifetime>,
    input: marker::PhantomData<fn() -> I>,
}

impl<I> Listener<I> {
    pub fn new<const COUNT: usize>(spec: tcp::Config) -> Self {
        Self {
            per_ip: None,
            connections: conn::Capacity::fixed::<COUNT>(),
            direct_flights: None,
            egress: egress::Config::DEFAULT,
            stream: Default::default(),
            transport: spec,
            connection_lifetime: None,
            input: marker::PhantomData,
        }
    }

    pub fn per_ip(mut self, limit: quota::PerIp) -> Self {
        self.per_ip = Some(limit);
        self
    }

    pub fn stream(mut self, stream: tcp::StreamConfig) -> Self {
        self.stream = stream;
        self
    }

    pub fn direct(mut self, count: usize) -> Self {
        self.direct_flights = Some(count.into());
        self
    }

    pub fn minimal(mut self) -> Self {
        self.direct_flights = Some(config::Flights::Minimum);
        self
    }

    pub const fn egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    pub const fn lifetime(mut self, life: handler::Lifetime) -> Self {
        self.connection_lifetime = Some(life);
        self
    }

    fn config<E>(&self) -> (config::Prepared<tcp::Tcp>, settings::Config)
    where
        E: env::Spec<Transport = tcp::Tcp>,
    {
        let flights = match self.direct_flights {
            Some(flights) => flights,
            None => self
                .connections
                .entries()
                .get()
                .min(DEFAULT_DIRECT_FLIGHTS)
                .into(),
        };
        let server = config::Prepared {
            connections: self.connections,
            direct_flights: flights,
            bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
            backlog: socket::Backlog::fixed::<128>(),
            stream: self.stream,
            per_ip: self.per_ip,
            transport: self.transport,
            egress: self.egress,
        };
        let config = settings::Config::for_profile::<E::Driver>();
        (server, config)
    }

    pub fn prepare<ID: Identity, A, E>(self, app: A) -> Result<Prepared<ID, E, A, I>, errors::Claim>
    where
        E: env::Spec<Transport = tcp::Tcp>,
        I: receive::Mode<E::Wire> + receive::Resources,
    {
        let (config, settings) = self.config::<E>();
        let mut install = Direct::<ID, E, _>::new(config, Value::<A, I>::new(app));
        if let Some(life) = self.connection_lifetime {
            install = install.with_life(life);
        }
        let plan = Plan::new((), install).with_install(settings)?;
        Ok(Prepared(plan))
    }
}

/// The exact listener installation and resolved descriptor layout, before execution.
pub struct Prepared<ID: Identity, E: env::Spec<Transport = tcp::Tcp>, A, I: receive::Resources>(
    topology::Prepared<(), Direct<ID, E, Value<A, I>>>,
);

impl<ID: Identity, A, W, E, I> Prepared<ID, E, (A, W), I>
where
    E: env::Spec<Transport = tcp::Tcp>,
    I: receive::Mode<E::Wire> + receive::Resources,
    <E::Wire as wire::Spec>::Bootstrap: domain::Init<E::Wire>,
    for<'d> <E::Wire as wire::Spec>::Binding<'d, ID>: From<W>,
    for<'d> A:
        handler::Application<'d, ID, Wire = E::Wire, Input = I> + handler::Runtime<'d, 'd, ID>,
{
    pub fn layout(&self) -> settings::Config {
        self.0.layout()
    }

    pub fn with_run<'report, R, B, Done>(
        self,
        complete: &'report mut Completion,
        body: B,
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(
            Result<
                Result<R, Failure<'report, Infallible, R>>,
                Kind<'owner, Infallible, &'owner Result<R, Failure<'report, Infallible, R>>>,
            >,
        ) -> Done,
    ) -> Done
    where
        B: for<'n, 'p, 's, 'app, 'life, 'd> FnOnce(
            &mut TcpCase<
                'app,
                'life,
                'd,
                app::Host<listener::Listener<'n, 'p, 's, 'd, 'd, ID, A, E>>,
            >,
        ) -> R,
    {
        self.0
            .enter_local(Listen(body, complete), policy, |outcome| match outcome {
                Ok(output) => finish(Ok(output)),
                Err(error) => finish(Err(error.kind())),
            })
    }
}

impl<'report, 'worker, 'owner, 'storage, 'driver, ID: Identity, A, W, E, I, B, R>
    ops::Operation<
        'driver,
        topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Direct<ID, E, Value<(A, W), I>>,
            (),
        >,
    > for Listen<'report, B>
where
    E: env::Spec<Transport = tcp::Tcp>,
    I: receive::Mode<E::Wire> + receive::Resources,
    <E::Wire as wire::Spec>::Bootstrap: domain::Init<E::Wire>,
    for<'d> <E::Wire as wire::Spec>::Binding<'d, ID>: From<W>,
    for<'d> A:
        handler::Application<'d, ID, Wire = E::Wire, Input = I> + handler::Runtime<'d, 'd, ID>,
    B: for<'n, 'p, 's, 'app, 'life, 'd> FnOnce(
        &mut TcpCase<'app, 'life, 'd, app::Host<listener::Listener<'n, 'p, 's, 'd, 'd, ID, A, E>>>,
    ) -> R,
{
    type Output = Result<R, Failure<'report, Infallible, R>>;

    fn run(
        self,
        worker: topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Direct<ID, E, Value<(A, W), I>>,
            (),
        >,
    ) -> Self::Output
    where
        Self: 'driver,
    {
        worker
            .with(self)
            .map_err(Failure::Install)?
            .map_err(Failure::Application)
    }
}
