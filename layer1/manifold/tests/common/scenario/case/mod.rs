use std::time::Duration;

use dope_core::driver::route::Identity;
use dope_runtime::executor::session::Completion;
pub mod attempt;
pub mod listener;
mod prepared;
mod value;

use std::{net, time};

use dope_core::driver::{settings, settings::errors};
use dope_manifold::{
    BuildError,
    app::env,
    client::app::request,
    network::client::topology::{
        Install,
        factory::{Application, disabled},
        plan,
    },
    service::{health, sync},
};
use dope_net::{
    link::{
        egress,
        pool::recv::{delivery, input, reserve::Choice},
    },
    tcp,
    wire::{self, receive::Policy},
};
use dope_runtime::{
    executor::{self, session::Error},
    topology::{self, Plan, lending, ops},
};
use o3::base::time::Positive;
pub use prepared::Prepared;
pub use value::Value;

use crate::common::scenario;

type Client<ID, const MAX: usize, E, F, Q> =
    Install<ID, MAX, E, F, plan::Standard, sync::Preserve, net::SocketAddr, 1, plan::Local, Q>;

#[derive(Debug)]
pub enum Failure<'report, E, R> {
    Install(BuildError<E>),
    Application(Error<'report, R>),
}

pub struct Connect<'report, F> {
    finish: &'report mut Completion,
    body: F,
}

impl<'report, 'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, A, F, R>
    lending::Next<lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>, A> for Connect<'report, F>
where
    A: executor::raw::App<'d>,
    F: FnOnce(&mut dyn scenario::RuntimeCase) -> R,
{
    type Output = Result<R, Error<'report, R>>;

    fn run(
        self,
        host: A,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, (), ()>,
    ) -> Self::Output {
        stage.with_quiesce(self.finish, host, Duration::from_secs(2), |mut app| {
            (self.body)(&mut app)
        })
    }
}

#[derive(Debug)]
pub enum PrepareError {
    Geometry(errors::Claim),
    Config(errors::Claim),
    Retry(health::Error),
}

impl<'report, 'worker, 'owner, 'storage, 'driver, ID: Identity, const MAX: usize, E, F, B, R>
    ops::Operation<
        'driver,
        topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Client<ID, MAX, E, F, request::Enabled>,
            (),
        >,
    > for Connect<'report, B>
where
    E: env::Spec<Transport = tcp::Tcp>,
    F: Application<(), ID, MAX, E, Input = plan::Local>,
    B: FnOnce(&mut dyn scenario::RuntimeCase) -> R,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
        Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
{
    type Output = Result<R, Failure<'report, F::Error, R>>;

    fn run(
        self,
        worker: topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Client<ID, MAX, E, F, request::Enabled>,
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

impl<'report, 'worker, 'owner, 'storage, 'driver, ID: Identity, const MAX: usize, E, F, B, R>
    ops::Operation<
        'driver,
        topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Client<ID, MAX, E, F, request::Disabled>,
            (),
        >,
    > for Connect<'report, B>
where
    E: env::Spec<Transport = tcp::Tcp>,
    F: disabled::Application<(), ID, MAX, E, Input = plan::Local>,
    B: FnOnce(&mut dyn scenario::RuntimeCase) -> R,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
        Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
    <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
{
    type Output = Result<R, Failure<'report, F::Error, R>>;

    fn run(
        self,
        worker: topology::Worker<
            'worker,
            'owner,
            'storage,
            'driver,
            (),
            Client<ID, MAX, E, F, request::Disabled>,
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

pub struct Connector<const MAX: usize> {
    address: net::SocketAddr,
    base: time::Duration,
    max: time::Duration,
    egress: egress::Config,
    life: Option<Positive>,
}

impl<const MAX: usize> Connector<MAX> {
    pub const fn new(address: net::SocketAddr, base: time::Duration, max: time::Duration) -> Self {
        Self {
            address,
            base,
            max,
            egress: egress::Config::DEFAULT,
            life: None,
        }
    }

    pub const fn egress(mut self, config: egress::Config) -> Self {
        self.egress = config;
        self
    }

    pub const fn lifetime(mut self, life: Positive) -> Self {
        self.life = Some(life);
        self
    }

    fn plan<ID: Identity>(&self) -> Result<plan::Plan<ID, MAX>, PrepareError> {
        let mut spec = plan::Spec::new(self.base, self.max)
            .map_err(PrepareError::Retry)?
            .with_egress(self.egress);
        if let Some(life) = self.life {
            spec = spec.with_ttl(life);
        }
        plan::Plan::new(MAX, spec).map_err(PrepareError::Geometry)
    }

    pub fn prepare<ID: Identity, F, E>(
        self,
        build: F,
    ) -> Result<Prepared<ID, MAX, E, F, request::Enabled>, PrepareError>
    where
        E: env::Spec<Transport = tcp::Tcp>,
        F: Application<(), ID, MAX, E, Input = plan::Local>,
        <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
            Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
        <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
    {
        let config = settings::Config::for_profile::<E::Driver>();
        use dope_manifold::service::{Endpoint, Revision, Snapshot};

        let catalog =
            Snapshot::from_one(Revision::new(1), Endpoint::new(self.address, self.address));
        let install = Install::<
            ID,
            MAX,
            E,
            _,
            plan::Standard,
            sync::Preserve,
            net::SocketAddr,
            1,
            plan::Local,
            request::Enabled,
        >::from_plan(catalog, self.plan()?, build);
        let prepared = Plan::new((), install)
            .with_install(config)
            .map_err(PrepareError::Config)?;
        Ok(Prepared::new(prepared))
    }

    pub fn prepare_off<ID: Identity, F, E>(
        self,
        build: F,
    ) -> Result<Prepared<ID, MAX, E, F, request::Disabled>, PrepareError>
    where
        E: env::Spec<Transport = tcp::Tcp>,
        F: disabled::Application<(), ID, MAX, E, Input = plan::Local>,
        <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Reserve:
            Choice<E::Wire, <E::Wire as input::Execution>::Retained>,
        <<E::Wire as wire::Spec>::Receive as Policy<E::Wire>>::Credit: delivery::Ownership<E::Wire>,
    {
        let config = settings::Config::for_profile::<E::Driver>();
        use dope_manifold::service::{Endpoint, Revision, Snapshot};

        let catalog =
            Snapshot::from_one(Revision::new(1), Endpoint::new(self.address, self.address));
        let install = Install::<
            ID,
            MAX,
            E,
            _,
            plan::Standard,
            sync::Preserve,
            net::SocketAddr,
            1,
            plan::Local,
            request::Disabled,
        >::from_plan(catalog, self.plan()?, build);
        let prepared = Plan::new((), install)
            .with_install(config)
            .map_err(PrepareError::Config)?;
        Ok(Prepared::new(prepared))
    }
}
