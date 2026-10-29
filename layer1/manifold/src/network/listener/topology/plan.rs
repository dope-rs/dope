//! Statically composed listener topology plan.
use std::marker;

use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_net::wire::{self, domain};
use dope_runtime::{
    executor::{self, session},
    shutdown,
    topology::{self, lending, recipe},
};
use listener::{config, handler, topology::factory};

use crate::{
    app::{self, env, receive},
    network::listener,
};

/// Typed topology plan owning configuration until installation.
/// Its installed form adds no graph node, box, or erased dispatch.
pub struct Plan<ID: Identity, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec,
{
    pub(super) config: config::Prepared<E::Transport>,
    pub(super) bound: Option<<E::Transport as dope_net::Listen>::BoundListener>,
    pub(super) lifetime: Option<handler::Lifetime>,
    pub(super) factory: F,
    env: marker::PhantomData<(ID, E, E::Wire, F::Input)>,
}

impl<ID: Identity, E, F> Plan<ID, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec,
{
    pub fn try_new(
        config: config::Config<E::Transport>,
        make: F,
    ) -> Result<Self, crate::ConfigError> {
        Ok(Self::new(config.try_prepare()?, make))
    }

    /// Consumes validated listener configuration without repeating admission.
    pub const fn new(config: config::Prepared<E::Transport>, make: F) -> Self {
        Self {
            config,
            bound: None,
            lifetime: None,
            factory: make,
            env: marker::PhantomData,
        }
    }

    #[must_use]
    pub fn with_bound(
        mut self,
        bound: Option<<E::Transport as dope_net::Listen>::BoundListener>,
    ) -> Self {
        self.bound = bound;
        self
    }

    #[must_use]
    pub const fn with_life(mut self, life: handler::Lifetime) -> Self {
        self.lifetime = Some(life);
        self
    }
}

impl<ID: Identity, E, F> settings::Resource for Plan<ID, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec + settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        use listener::topology::Layout;

        let needs = Layout::<ID, F::Input>::new(self.config.connections()).include_in(needs)?;
        self.factory.include_in(needs)
    }
}

impl<S, ID: Identity, E, F> recipe::Execute<S> for Plan<ID, E, F>
where
    S: storage::Plan,
    E: env::Spec,
    E::Transport: dope_net::Listen,
    <E::Wire as wire::Spec>::Bootstrap: domain::Bootstrap<E::Wire>,
    F: factory::Application<S, ID, E>,
    F::Input: receive::Mode<E::Wire>,
{
    type Error = crate::BuildError<F::Error>;

    fn execute<'report, 'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
        report: &'report mut session::Completion,
    ) -> Result<
        shutdown::Requested<Q>,
        topology::Failure<'report, Self::Error, shutdown::Requested<Q>>,
    >
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: executor::Startup,
    {
        lending::Run(report).install(self, stage)
    }
}

impl<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, S, Q, N, O, ID: Identity + 'd, E, F>
    lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>, N> for Plan<ID, E, F>
where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    Self: 'd,
    E: env::Spec,
    E::Transport: dope_net::Listen,
    <E::Wire as wire::Spec>::Bootstrap: domain::Bootstrap<E::Wire>,
    F: factory::Application<S, ID, E>,
    F::Input: receive::Mode<E::Wire>,
    N: for<'n, 'p, 's> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
            app::Host<
                listener::Listener<'n, 'p, 's, 'life, 'd, ID, F::Application<'life, 'e, 'd>, E>,
            >,
            Output = O,
        >,
{
    type Output = O;
    type Error = crate::BuildError<F::Error>;

    fn install(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        let (app, end) = self
            .factory
            .build(stage.storage())
            .map_err(crate::BuildError::Factory)?;
        let install = listener::Install::<ID, E, _>::new(self.config, end).with_bound(self.bound);
        let install = match self.lifetime {
            Some(life) => install.with_life(life),
            None => install,
        };
        install
            .in_stage(stage, app, |listener, stage| {
                next.run(app::Host::new(listener), stage)
            })
            .map_err(crate::BuildError::Create)
    }
}
