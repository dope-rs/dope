use crate::proto::datagram;
use crate::runtime::endpoint::owner;
use std::net;

use o3::buffer::resident;

use dope::core::driver;
use dope::manifold::network::datagram::Source;

use crate::conn::id;
use crate::conn::life::{accept, options};
use crate::errors;
use crate::runtime::endpoint::{self, config};
use crate::runtime::mux::{app, handler, router};
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
    bind: Source<'g>,
    route: L,
    source: rustls::ServerConfig,
    connection: options::Options,
    early: Option<early::ReplayCap>,
    endpoint: endpoint::Plan<ID, R>,
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
            Source::Bind(bind),
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
            Source::Bind(address),
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
        Ok(Plan::prepare(Source::Adopt(lease), route, source, opts, ep))
    }
}

impl<'g, const ID: u8, R: receive::Policy, L: routing::LocalRoute> Plan<'g, ID, R, L> {
    /// Enables segmentation before this server admits traffic; the default is off.
    pub fn enable_gso(mut self) -> Result<Self, crate::runtime::mux::manage::GsoError> {
        self.endpoint.config.capacity.gso = Some(crate::runtime::mux::manage::gso_limits()?);
        Ok(self)
    }

    fn prepare(
        bind: Source<'g>,
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
        self.bind_runtime(driver, |_| Ok((events, endpoint::Idle::new())))
    }

    fn bind_runtime<'pool, 'd, H, A, E>(
        self,
        driver: &mut driver::Context<'_, 'd>,
        build: impl FnOnce(driver::Binding<'d>) -> Result<(H, A), E>,
    ) -> endpoint::Bound<'d, ID, H, R, A, owner::Owned<L>, datagram::Sealed<'pool>, E>
    where
        'g: 'd,
        E: From<endpoint::BindError>,
        H: handler::Handler<ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>,
        H: cert::Sink<ID, id::Driver<'d>>,
        <H::Events as Dispatch<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>>>::Packet:
            packet::Mode<H, ID, R::Buffer<'d>, resident::Shared<'d>, id::Driver<'d>, Output = ()>,
    {
        let binding = driver::Binding::new(driver.driver_ref().timer(), self.endpoint.owners());
        let auth = accept::Authority::from_parts(self.source, self.connection, self.early)
            .map_err(endpoint::BindError::Connection)?;
        let state = self
            .endpoint
            .mux()
            .bind(driver, self.route, Some(auth))
            .map_err(endpoint::BindError::Mux)?;
        let socket = self.endpoint.open(self.bind, driver)?;
        let (handler, app) = build(binding)?;
        Ok(endpoint::Endpoint::new(
            socket,
            router::Router { state, handler },
            app,
        ))
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
        self.bind_runtime(driver, |binding| {
            let (events, app) = bind(binding).map_err(endpoint::BindFailure::Application)?;
            Ok((events, endpoint::Run::<A, Q>::new(app)))
        })
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
