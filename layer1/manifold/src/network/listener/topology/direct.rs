use std::marker;

use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_runtime::{
    executor::{self, session},
    shutdown,
    topology::{self, lending, recipe},
};
use listener::{
    config, handler,
    topology::{Layout, factory},
};

use crate::{
    app::{self, env, receive},
    network::listener,
};

/// Listener topology whose application is owned directly by its factory.
pub struct Direct<ID: Identity, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec,
{
    config: config::Prepared<E::Transport>,
    bound: Option<<E::Transport as dope_net::Listen>::BoundListener>,
    lifetime: Option<handler::Lifetime>,
    factory: F,
    env: marker::PhantomData<(ID, E, E::Wire, F::Input)>,
}

impl<ID: Identity, E, F> Direct<ID, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec,
{
    pub const fn new(config: config::Prepared<E::Transport>, factory: F) -> Self {
        Self {
            config,
            bound: None,
            lifetime: None,
            factory,
            env: marker::PhantomData,
        }
    }

    /// Lends the installed listener to an inferred closure.
    ///
    /// Typed continuations remain available through [`lending::Installer::install`].
    pub fn install<'stage, 'scope, 'life, 'e, 'd, S, Q, N, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        next: N,
    ) -> Result<O, crate::BuildError<F::Error>>
    where
        ID: 'd,
        S: storage::Plan,
        Q: executor::Startup,
        Self: 'd,
        F: factory::Direct<ID, E>,
        F::Input: receive::Mode<E::Wire>,
        N: for<'n, 'p, 's> FnOnce(
            app::Host<listener::Listener<'n, 'p, 's, 'd, 'd, ID, F::Application<'d>, E>>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> O,
    {
        let (app, wire) = self.factory.build().map_err(crate::BuildError::Factory)?;
        let install = listener::Install::<ID, E, _>::new(self.config, wire).with_bound(self.bound);
        let install = match self.lifetime {
            Some(life) => install.with_life(life),
            None => install,
        };
        install
            .in_stage(stage, app, |listener, _wait, stage| {
                next(app::Host::new(listener), stage)
            })
            .map_err(crate::BuildError::Create)
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

impl<ID: Identity, E, F> settings::Resource for Direct<ID, E, F>
where
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::ReceiveSpec + settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = Layout::<ID, F::Input>::new(self.config.connections()).include_in(needs)?;
        self.factory.include_in(needs)
    }
}

impl<S, ID: Identity, E, F> recipe::Execute<S> for Direct<ID, E, F>
where
    S: storage::Plan,
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::Direct<ID, E>,
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

impl<'stage, 'scope, 'life, 'e, 'd, S, Q, N, O, ID: Identity + 'd, E, F>
    lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>, N> for Direct<ID, E, F>
where
    S: storage::Plan,
    Q: executor::Startup,
    Self: 'd,
    E: env::Spec,
    E::Transport: dope_net::Listen,
    F: factory::Direct<ID, E>,
    F::Input: receive::Mode<E::Wire>,
    N: for<'n, 'p, 's> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
            app::Host<listener::Listener<'n, 'p, 's, 'd, 'd, ID, F::Application<'d>, E>>,
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
        self.install(stage, |app, stage| next.run(app, stage))
    }
}
