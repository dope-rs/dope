use crate::proto::datagram;
use crate::runtime::endpoint::owner;
use std::net;

use o3::buffer::resident;

use dope::core::{driver, io::datagram::Lease};

use crate::conn::id;
use crate::conn::life::{accept, options};
use crate::errors;
use crate::runtime::endpoint::{self, config};
use crate::runtime::mux::{app, builder, handler};
use crate::runtime::receive;
use crate::{
    proto::{early, param},
    runtime::routing,
};
use handler::hook::{
    set::Dispatch,
    wire::{cert, packet},
};

/// Single-use server auth and receive resource plan.
#[must_use = "a server plan must be consumed by one endpoint binding"]
pub struct Plan<
    'g,
    const ID: u8,
    R: receive::Policy = receive::Retained,
    L: routing::LocalRoute = routing::Plain,
> {
    bind: Socket<'g>,
    route: L,
    source: rustls::ServerConfig,
    connection: options::Options,
    early: Option<early::ReplayCap>,
    endpoint: endpoint::Plan<ID, R>,
}

enum Socket<'g> {
    Bind(net::SocketAddr),
    Adopt(Lease<'g>),
}

impl<const ID: u8, R: receive::Policy> Plan<'static, ID, R> {
    pub fn new(
        bind: net::SocketAddr,
        source: rustls::ServerConfig,
        opts: options::Options,
        config: config::Config,
    ) -> Result<Self, endpoint::PlanError> {
        let ep = endpoint::Plan::new(config).map_err(endpoint::PlanError::Config)?;
        Ok(Self::prepare(
            Socket::Bind(bind),
            routing::Plain,
            source,
            opts,
            ep,
        ))
    }

    pub fn from_params(
        bind: net::SocketAddr,
        source: rustls::ServerConfig,
        params: param::Set,
        config: config::Config,
    ) -> Result<Self, endpoint::PlanError> {
        let opts = options::Options::try_from(params)
            .map_err(errors::Connect::Options)
            .map_err(endpoint::PlanError::Connection)?;
        Self::new(bind, source, opts, config)
    }

    pub fn with_early(
        bind: net::SocketAddr,
        source: rustls::ServerConfig,
        opts: options::Options,
        cap: early::ReplayCap,
        config: config::Config,
    ) -> Result<Self, endpoint::PlanError> {
        let mut plan = Self::new(bind, source, opts, config)?;
        plan.early = Some(cap);
        Ok(plan)
    }

    pub fn single(
        bind: routing::Single,
        source: rustls::ServerConfig,
        opts: options::Options,
        config: config::Config,
    ) -> Result<Plan<'static, ID, R, routing::SingleRoute>, endpoint::PlanError> {
        let ep = endpoint::Plan::single(config, &bind).map_err(endpoint::PlanError::Config)?;
        let (address, route) = bind.into_parts();
        Ok(Plan::prepare(
            Socket::Bind(address),
            route,
            source,
            opts,
            ep,
        ))
    }

    /// Consumes one routed socket and owns its TLS authority through endpoint retirement.
    pub fn routed<'g>(
        bind: routing::Binding<'g>,
        source: rustls::ServerConfig,
        opts: options::Options,
        config: config::Config,
    ) -> Result<Plan<'g, ID, R, routing::Routed>, endpoint::PlanError> {
        let ep = endpoint::Plan::routed(config, &bind).map_err(endpoint::PlanError::Config)?;
        let (lease, route) = bind.into_parts();
        Ok(Plan::prepare(Socket::Adopt(lease), route, source, opts, ep))
    }
}

impl<'g, const ID: u8, R: receive::Policy, L: routing::LocalRoute> Plan<'g, ID, R, L> {
    /// Enables segmentation before this server admits traffic; the default is off.
    pub fn enable_gso(mut self) -> Result<Self, crate::runtime::mux::manage::GsoError> {
        self.endpoint.config.capacity.gso = Some(crate::runtime::mux::manage::gso_limits()?);
        Ok(self)
    }

    fn prepare(
        bind: Socket<'g>,
        route: L,
        source: rustls::ServerConfig,
        mut opts: options::Options,
        ep: endpoint::Plan<ID, R>,
    ) -> Self {
        opts.cap_udp(ep.packet());
        Self {
            bind,
            route,
            source,
            connection: opts,
            early: None,
            endpoint: ep,
        }
    }

    pub fn bind<'pool, 'd, H>(
        self,
        events: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> endpoint::Bound<'d, ID, H, R, endpoint::Idle, owner::Owned<L>, datagram::Sealed<'pool>>
    where
        'g: 'd,
        H: handler::Handler<ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
        H: cert::Sink<ID, id::Driver<'d>>,
        <H::Events as Dispatch<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>>::Packet:
            packet::Mode<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
    {
        self.bind_runtime(events, endpoint::Idle::new(), driver)
    }

    fn bind_runtime<'pool, 'd, H, A>(
        self,
        events: H,
        app: A,
        driver: &mut driver::Context<'_, 'd>,
    ) -> endpoint::Bound<'d, ID, H, R, A, owner::Owned<L>, datagram::Sealed<'pool>>
    where
        'g: 'd,
        H: handler::Handler<ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
        H: cert::Sink<ID, id::Driver<'d>>,
        <H::Events as Dispatch<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>>::Packet:
            packet::Mode<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
    {
        let auth = accept::Authority::from_parts(self.source, self.connection, self.early)
            .map_err(endpoint::BindError::Connection)?;
        let mux = builder::Builder::server_in(events, auth, self.route, self.endpoint.mux())
            .bind(driver)
            .map_err(endpoint::BindError::Mux)?;
        let ep = self.endpoint.prepare(driver)?;
        match self.bind {
            Socket::Bind(address) => ep.bind(address, mux, app, driver),
            Socket::Adopt(lease) => ep.adopt(lease, mux, app, driver),
        }
    }

    pub fn bind_with<'pool, 'd, Q, H, A, F, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        bind: F,
    ) -> endpoint::Bound<
        'd,
        ID,
        H,
        R,
        endpoint::Run<A, Q>,
        owner::Owned<L>,
        datagram::Sealed<'pool>,
        endpoint::BindFailure<E>,
    >
    where
        'g: 'd,
        H: handler::Handler<ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
        H: cert::Sink<ID, id::Driver<'d>>,
        <H::Events as Dispatch<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>>::Packet:
            packet::Mode<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
        A: app::Driver<'d, H, ID, R::Buffer<'d>, resident::Shared<'d>>,
        Q: app::Receipt<'d, H, ID, R::Buffer<'d>, resident::Shared<'d>, A>,
        F: FnOnce(driver::Binding<'d>) -> Result<(H, A), E>,
    {
        let binding = driver::Binding::new(driver.driver_ref().timer(), self.endpoint.owners());
        let (events, app) = bind(binding).map_err(endpoint::BindFailure::Application)?;
        self.bind_runtime(events, endpoint::Run::<A, Q>::new(app), driver)
            .map_err(endpoint::BindFailure::Endpoint)
    }
}

impl<const ID: u8, R: receive::Policy, L: routing::LocalRoute> AsRef<endpoint::Plan<ID, R>>
    for Plan<'_, ID, R, L>
{
    fn as_ref(&self) -> &endpoint::Plan<ID, R> {
        &self.endpoint
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Plan<0, receive::Copied>>()
            == std::mem::size_of::<Plan<0, receive::Retained>>()
    );
};
