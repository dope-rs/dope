use std::{fmt::Debug, time::Duration};

use dope_core::driver::{
    life::recovery,
    route::{Identity, Route},
};
use dope_runtime::executor::session::Completion;
use dope_test::reclaim::Reports;

pub mod attempt;
pub mod listener;
mod prepared;
mod value;

use std::{net, time};

use dope_core::driver::{settings, settings::errors};
use dope_manifold::{
    BuildError,
    app::{Host, env},
    network::client::{
        self,
        topology::{Install, factory::Application, plan},
    },
    service::{health, sync},
};
use dope_net::{link::egress, tcp};
use dope_runtime::{
    executor::{self, session::Error},
    topology::{self, Plan, lending, ops},
};
use o3::base::time::Positive;
pub use prepared::Prepared;
pub use value::Value;

use crate::common::scenario;

type Client<ID, const MAX: usize, E, F> =
    Install<ID, MAX, E, F, plan::Standard, sync::Preserve, net::SocketAddr, 1, plan::Local>;

pub(crate) type ClientHost<'q, 'h, 'p, 'd, ID, const MAX: usize, N, E> =
    Host<client::Pool<'q, 'h, 'p, 'd, 'd, ID, MAX, N, sync::Preserve, net::SocketAddr, E, 1>>;

#[derive(Debug)]
pub enum Failure<'report, E, R> {
    Install(BuildError<E>),
    Application(Error<'report, R>),
}

fn enter<I, O, R: Debug>(
    plan: topology::Prepared<(), I>,
    route: Route,
    operation: O,
) -> (R, Reports<1>)
where
    O: for<'scope, 'life, 'p, 'd> ops::Operation<
            'd,
            topology::Worker<'scope, 'life, 'p, 'd, (), I, ()>,
            Output = R,
        >,
{
    let mut reports = Reports::new(plan.layout(), [route]);
    let output = plan.enter_local(
        operation,
        recovery::Policy::new(|event| reports.push(event), crate::common::recover),
        |outcome| {
            match outcome {
                Ok(output) => Ok(output),
                Err(ref error) => Err(error.kind()),
            }
            .expect("native lifecycle")
        },
    );
    (output, reports)
}

fn checked<R, E: Debug>((result, reports): (Result<R, E>, Reports<1>)) -> R {
    let output = result.expect("application lifecycle");
    crate::common::manifold::client::assert_reclaimed(&reports);
    output
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
        topology::Worker<'worker, 'owner, 'storage, 'driver, (), Client<ID, MAX, E, F>, ()>,
    > for Connect<'report, B>
where
    E: env::Spec<Transport = tcp::Tcp>,
    F: Application<'owner, 'storage, 'driver, (), ID, MAX, E, Input = plan::Local>,
    B: FnOnce(&mut dyn scenario::RuntimeCase) -> R,
    for<'q, 'h, 'p> ClientHost<'q, 'h, 'p, 'driver, ID, MAX, F::Session, E>:
        executor::raw::App<'driver>,
{
    type Output = Result<R, Failure<'report, F::Error, R>>;

    fn run(
        self,
        worker: topology::Worker<'worker, 'owner, 'storage, 'driver, (), Client<ID, MAX, E, F>, ()>,
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
    ) -> Result<Prepared<ID, MAX, E, F>, PrepareError>
    where
        E: env::Spec<Transport = tcp::Tcp>,
    {
        let config = settings::Config::for_profile::<E::Driver>();
        use dope_manifold::service::{Endpoint, Revision, Snapshot};

        let catalog =
            Snapshot::from_one(Revision::new(1), Endpoint::new(self.address, self.address));
        let install = Client::<ID, MAX, E, F>::from_plan(catalog, self.plan()?, build);
        let prepared = Plan::new((), install)
            .with_install(config)
            .map_err(PrepareError::Config)?;
        Ok(Prepared::new(prepared))
    }
}
