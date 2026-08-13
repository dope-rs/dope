use std::{io, net, pin, time};

use dope_core::driver::{self, ops::Buffers as _, schedule};
use dope_net::{link::egress, tcp, wire};

use crate::{
    connector::{auxiliary, connection, lifecycle, session},
    service::{self, discover, health, observe, reconcile},
    timing,
};

mod sealed;

type Addr<E> = <<E as crate::Env>::Transport as dope_net::Transport>::Addr;
type Core<'d, const ID: u8, N, R, I, E, const ENDPOINTS: usize, X> = connection::Engine<
    'd,
    ID,
    session::Application<'d, ID, N, <E as crate::Env>::Wire>,
    service::Pool<<E as crate::Env>::Transport, R, I, ENDPOINTS>,
    E,
    X,
>;

pub struct Config<O, W> {
    connections: usize,
    backoff: health::Backoff,
    observer: O,
    wire: W,
    egress: egress::Config,
    ingress_capacity: Option<usize>,
}

impl<O, W> Config<O, W> {
    pub const fn new(connections: usize, backoff: health::Backoff, observer: O, wire: W) -> Self {
        Self {
            connections,
            backoff,
            observer,
            wire,
            egress: egress::Config::DEFAULT,
            ingress_capacity: None,
        }
    }

    /// Caps bytes retained by the protocol parser across every connection.
    /// The default is derived from the driver's receive arena.
    pub const fn with_ingress_capacity(mut self, capacity: usize) -> Self {
        self.ingress_capacity = Some(capacity);
        self
    }

    pub const fn with_egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }
}

/// Discovery-driven connection owner for one logical service.
#[pin_project::pin_project]
pub struct Connector<
    'd,
    const ID: u8,
    const MAX_CONNECTIONS: usize,
    N,
    D,
    R,
    I = net::SocketAddr,
    O = observe::Ignore,
    E = crate::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>,
    const ENDPOINTS: usize = 16,
    X = auxiliary::Disabled,
> where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
    X: auxiliary::Mode<'d, N::Send, ID>,
{
    #[pin]
    engine: Core<'d, ID, N, R, I, E, ENDPOINTS, X>,
    discovery: D,
    observer: O,
    poll_at: Option<time::Instant>,
}

impl<'d, const ID: u8, const MAX_CONNECTIONS: usize, N, R, I, E, const ENDPOINTS: usize, D, O>
    Connector<'d, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS, auxiliary::Disabled>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
{
    pub fn new(
        session: N,
        discovery: D,
        config: Config<O, <E::Wire as wire::Wire>::InitConfig<'d, ID>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_mode(session, discovery, config, auxiliary::Disabled, driver)
    }
}

impl<'d, const ID: u8, const MAX_CONNECTIONS: usize, N, R, I, E, const ENDPOINTS: usize, D, O, C>
    Connector<'d, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS, auxiliary::Enabled<C>>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
    C: auxiliary::Control<'d, N::Send, ID>,
{
    pub fn new_with_auxiliary(
        session: N,
        auxiliary: C,
        discovery: D,
        config: Config<O, <E::Wire as wire::Wire>::InitConfig<'d, ID>>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_mode(
            session,
            discovery,
            config,
            auxiliary::Enabled::new(auxiliary),
            driver,
        )
    }
}

impl<'d, const ID: u8, const MAX_CONNECTIONS: usize, N, R, I, E, const ENDPOINTS: usize, D, O, X>
    Connector<'d, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
    X: auxiliary::Mode<'d, N::Send, ID>,
{
    fn with_mode(
        session: N,
        mut discovery: D,
        config: Config<O, <E::Wire as wire::Wire>::InitConfig<'d, ID>>,
        auxiliary: X,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        let Config {
            connections,
            backoff,
            observer,
            wire: wire_config,
            egress,
            ingress_capacity,
        } = config;
        use dope_core::driver::route::table;

        if connections > MAX_CONNECTIONS || table::ConnectionCapacity::new(connections).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "dope: service connections must be in 1..={MAX_CONNECTIONS} and fit the target table"
                ),
            ));
        }
        let now = driver.turn_now();
        discovery.refresh(now, discover::Refresh::Startup);
        let options = <E::Transport as dope_net::Transport>::stream_options(Default::default())?;
        let controller = service::Pool::try_new(connections, backoff, options)?;
        let recv_capacity = driver
            .buffer_count()
            .checked_mul(driver.buffer_len())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "receive capacity overflow")
            })?;
        let ingress_capacity = match ingress_capacity {
            Some(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dope: connector ingress capacity must be positive",
                ));
            }
            Some(capacity) => capacity,
            None => recv_capacity.max(session::DEFAULT_INGRESS_CAP),
        };
        let app = session::Application::new(session, ingress_capacity, driver.region_token_ref());
        connection::Engine::with_app_configs_mode(
            app,
            controller,
            auxiliary,
            connections,
            egress,
            wire_config,
            driver,
        )
        .map(|engine| Self {
            engine,
            discovery,
            observer,
            poll_at: Some(now),
        })
    }

    pub fn session(&self) -> &N {
        self.engine.app.session()
    }

    pub fn session_mut(self: pin::Pin<&mut Self>) -> &mut N {
        self.project().engine.project().app.session_mut()
    }

    fn discovery_due(self: pin::Pin<&Self>, now: time::Instant) -> bool {
        let refresh = self.as_ref().has_discovery_refresh();
        let this = self.project_ref();
        D::REACTIVE || this.poll_at.is_some_and(|deadline| deadline <= now) || refresh
    }

    fn poll_discovery<'turn>(
        mut self: pin::Pin<&mut Self>,
        now: time::Instant,
        permit: schedule::MaintenancePermit<'turn, 'd>,
    ) {
        if D::REACTIVE && self.as_ref().project_ref().discovery.changed() {
            *self.as_mut().project().poll_at = Some(now);
        }
        if let Some(cause) = self.as_mut().take_discovery_refresh() {
            let this = self.as_mut().project();
            this.discovery
                .refresh(now, discover::Refresh::Failure(cause));
            *this.poll_at = Some(now);
        }
        if self
            .as_ref()
            .project_ref()
            .poll_at
            .is_none_or(|deadline| deadline > now)
        {
            return;
        }

        let action = self.as_mut().project().discovery.poll(now);
        match action {
            discover::Action::Pending { poll_at } => {
                *self.project().poll_at = poll_at;
            }
            discover::Action::Published {
                snapshot,
                metadata,
                poll_at,
            } => {
                let revision = snapshot.revision();
                {
                    let this = self.as_mut().project();
                    this.observer.resolved(&snapshot, &metadata);
                }
                let result = self.as_mut().reconcile(snapshot, permit);
                let this = self.project();
                *this.poll_at = poll_at;
                match result {
                    Ok(change) => this.observer.reconciled(revision, change),
                    Err(error) => this.observer.rejected(error),
                }
            }
            discover::Action::Expired { revision, poll_at } => {
                let snapshot = service::Snapshot::empty(revision);
                self.as_mut().project().observer.expired(revision);
                let result = self.as_mut().reconcile(snapshot, permit);
                let this = self.project();
                *this.poll_at = poll_at;
                match result {
                    Ok(change) => this.observer.reconciled(revision, change),
                    Err(error) => this.observer.rejected(error),
                }
            }
            discover::Action::Failed { error, poll_at } => {
                let this = self.project();
                this.observer.failed(&error);
                *this.poll_at = poll_at;
            }
        }
    }
}

impl<'d, const ID: u8, const MAX_CONNECTIONS: usize, N, R, I, E, const ENDPOINTS: usize, D, O, X>
    Connector<'d, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS>,
    E: crate::Env,
    E::Transport: dope_net::Transport,
    I: Eq,
    R: reconcile::Policy,
    D: discover::Discover<I, Addr<E>, ENDPOINTS>,
    O: observe::Observe<I, Addr<E>, D::Metadata, D::Error, ENDPOINTS>,
    X: auxiliary::Mode<'d, N::Send, ID>,
{
    /// Atomically reconciles desired endpoint state under the service policy.
    pub(super) fn reconcile<'turn>(
        mut self: pin::Pin<&mut Self>,
        snapshot: service::Snapshot<service::Endpoint<I, Addr<E>>, ENDPOINTS>,
        _permit: schedule::MaintenancePermit<'turn, 'd>,
    ) -> Result<service::Change, service::ReconcileError> {
        let mut engine = self.as_mut().project().engine;
        let capacity = engine.as_ref().connection_capacity();
        let change = engine
            .as_mut()
            .controller_mut()
            .reconcile(snapshot, capacity)?;
        engine.as_ref().wake_controller();
        Ok(change)
    }

    pub(super) fn apply_retirements(mut self: pin::Pin<&mut Self>, turn: schedule::Turn<'_, 'd>) {
        let mut engine = self.as_mut().project().engine;
        if !engine.as_ref().controller().has_retirements() {
            return;
        }
        let share = turn.maintenance().half();
        loop {
            if share.remaining() == 0 {
                break;
            }
            let binding = {
                let capacity = engine.as_ref().connection_capacity();
                engine.as_mut().controller_mut().next_retirement(capacity)
            };
            let Some(binding) = binding else {
                break;
            };
            if !share.take() {
                break;
            }
            let Some(attempt) = engine.as_ref().attempt_at(binding) else {
                continue;
            };
            if !engine.as_ref().controller().should_retire(attempt) {
                continue;
            }
            engine.as_mut().request_close_attempt(
                binding,
                attempt,
                lifecycle::CloseReason::EndpointRetired,
            );
        }
    }

    pub(super) fn has_retirements(self: pin::Pin<&Self>) -> bool {
        self.project_ref().engine.controller().has_retirements()
    }

    pub(super) fn has_discovery_refresh(self: pin::Pin<&Self>) -> bool {
        self.project_ref().engine.controller().has_refresh()
    }

    pub(super) fn take_discovery_refresh(self: pin::Pin<&mut Self>) -> Option<health::Cause> {
        self.project().engine.controller_mut().take_refresh()
    }
}
