//! Statically composed service connector topology.
use std::{marker, net};

use dope_core::driver::{
    route::Identity,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_net::{
    link::pool::recv::{delivery, input, reserve::Choice},
    wire::{self, receive::Policy},
};
use dope_runtime::{executor, topology::lending};

use crate::{
    app,
    app::env,
    client::{app::request, side, side::registry},
    network::client,
    service,
    service::{health, sync},
};

pub mod factory;
pub mod plan;
mod recipe;

use factory::disabled;
pub use recipe::Recipe;

/// Construction and installation retain the same side registry loan.
type Registered<'life, 'd, ID, I> = side::Enabled<&'life registry::Registry<'d, I, ID>>;

/// One statically selected connector installation.
pub struct Install<
    ID: Identity,
    const MAX: usize,
    E,
    F,
    M = plan::Standard,
    R = sync::Preserve,
    I = net::SocketAddr,
    const ENDPOINTS: usize = 16,
    A = plan::Local,
    Q = request::Disabled,
> where
    E: env::Spec,
{
    set: service::Snapshot<
        service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
        ENDPOINTS,
    >,
    plan: plan::Plan<ID, MAX, M, A>,
    make: F,
    types: marker::PhantomData<(ID, E, E::Wire, R, Q)>,
}

impl<ID: Identity, const MAX: usize, E, F, M, R, I, const ENDPOINTS: usize, A, Q>
    Install<ID, MAX, E, F, M, R, I, ENDPOINTS, A, Q>
where
    E: env::Spec,
{
    pub const fn from_plan(
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        plan: plan::Plan<ID, MAX, M, A>,
        make: F,
    ) -> Self {
        Self {
            set,
            plan,
            make,
            types: marker::PhantomData,
        }
    }
}

impl<ID: Identity, const MAX: usize, E, F, R, I, const ENDPOINTS: usize, A, Q>
    Install<ID, MAX, E, F, plan::Standard, R, I, ENDPOINTS, A, Q>
where
    E: env::Spec,
    A: plan::Input,
{
    pub fn new(
        set: service::Snapshot<
            service::Endpoint<I, <<E as env::Spec>::Transport as dope_net::Transport>::Addr>,
            ENDPOINTS,
        >,
        cap: usize,
        spec: plan::Spec,
        make: F,
    ) -> Result<Self, errors::Claim> {
        Ok(Self::from_plan(set, plan::Plan::new(cap, spec)?, make))
    }
}

impl<ID: Identity, const MAX: usize, E, F, M, R, I, const ENDPOINTS: usize, A, Q> settings::Resource
    for Install<ID, MAX, E, F, M, R, I, ENDPOINTS, A, Q>
where
    E: env::Spec,
    plan::Plan<ID, MAX, M, A>: settings::Resource,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.plan.include_in(needs)
    }
}

impl<ID: Identity, const MAX: usize, E, F, R, I, const ENDPOINTS: usize, A>
    Install<ID, MAX, E, F, plan::Standard, R, I, ENDPOINTS, A, request::Enabled>
where
    E: env::Spec,
    E::Transport: dope_net::Transport,
    A: plan::Input,
    I: Eq,
    R: sync::Policy,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
        Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
{
    pub fn install<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, S, Q, O>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        next: impl for<'q, 'h, 'p> FnOnce(
            app::Host<
                client::Pool<
                    'q,
                    'h,
                    'p,
                    'd,
                    'd,
                    ID,
                    MAX,
                    F::Session<'life, 'e, 'd>,
                    R,
                    I,
                    E,
                    ENDPOINTS,
                    side::Disabled,
                >,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> O,
    ) -> Result<O, crate::BuildError<F::Error>>
    where
        S: storage::Plan + 'd,
        Q: executor::Startup,
        Self: 'd,
        F: factory::Application<S, ID, MAX, E, Input = A>,
    {
        let Self {
            set,
            plan,
            make,
            types: _,
        } = self;
        let (setup, wire) = make
            .build(stage.storage())
            .map_err(crate::BuildError::Factory)?;
        let hash = stage.hash_state(health::Domain::for_id(ID::ID.raw()));
        let plan = plan.bind(wire, hash);
        plan.install::<_, _, R, I, E, ENDPOINTS, _, _>(
            setup,
            set,
            stage,
            lending::Stage::driver,
            |pool, stage| next(app::Host::new(pool), stage),
        )
        .map_err(crate::BuildError::Create)
    }
}

impl<
    'stage,
    'scope,
    'life: 'scope,
    'e: 'life,
    'd: 'e,
    S,
    Q,
    N,
    O,
    ID: Identity,
    const MAX: usize,
    E,
    F,
    R,
    I,
    const ENDPOINTS: usize,
    A,
> lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>, N>
    for Install<ID, MAX, E, F, plan::Standard, R, I, ENDPOINTS, A, request::Disabled>
where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    Self: 'd,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    A: plan::Input,
    F: disabled::Application<S, ID, MAX, E, Input = A>,
    I: Eq,
    R: sync::Policy,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
        Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
    N: for<'q, 'h, 'p> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
            app::Host<
                client::Pool<
                    'q,
                    'h,
                    'p,
                    'd,
                    'd,
                    ID,
                    MAX,
                    F::Session<'life, 'e, 'd>,
                    R,
                    I,
                    E,
                    ENDPOINTS,
                    side::Disabled,
                >,
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
        let Self {
            set,
            plan,
            make,
            types: _,
        } = self;
        let (setup, wire) = make
            .build(stage.storage())
            .map_err(crate::BuildError::Factory)?;
        let hash = stage.hash_state(health::Domain::for_id(ID::ID.raw()));
        let plan = plan.bind(wire, hash);
        plan.install::<_, _, R, I, E, ENDPOINTS, _, _>(
            setup,
            set,
            stage,
            lending::Stage::driver,
            |pool, stage| next.run(app::Host::new(pool), stage),
        )
        .map_err(crate::BuildError::Create)
    }
}

impl<
    'stage,
    'scope,
    'life: 'scope,
    'e: 'life,
    'd: 'e,
    S,
    Q,
    N,
    O,
    ID: Identity,
    const MAX: usize,
    E,
    F,
    R,
    I,
    const ENDPOINTS: usize,
    A,
> lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>, N>
    for Install<ID, MAX, E, F, plan::Standard, R, I, ENDPOINTS, A, request::Enabled>
where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    Self: 'd,
    E: env::Spec,
    E::Transport: dope_net::Transport,
    A: plan::Input,
    F: factory::Application<S, ID, MAX, E, Input = A>,
    I: Eq,
    R: sync::Policy,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
        Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
    N: for<'q, 'h, 'p> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
            app::Host<
                client::Pool<
                    'q,
                    'h,
                    'p,
                    'd,
                    'd,
                    ID,
                    MAX,
                    F::Session<'life, 'e, 'd>,
                    R,
                    I,
                    E,
                    ENDPOINTS,
                    side::Disabled,
                >,
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
        self.install(stage, |app, stage| next.run(app, stage))
    }
}
