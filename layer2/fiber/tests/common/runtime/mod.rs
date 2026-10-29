use std::convert::Infallible;

use dope::{
    core::driver::{
        life::creation::Attempt,
        retained::Context,
        schedule::clock::Sample,
        settings::{Config, Schedule, demands, errors},
        storage::Value,
    },
    runtime::executor::Executor,
};
use dope_test::scope::{self, Driver};
mod empty;

pub use empty::Empty;
pub mod profile;

use std::time::Instant;

use dope::{
    core::driver::{
        self,
        life::{self, domain, recovery},
        retained,
        schedule::state::scope::Guard,
        settings::{self, claim::Needs, demands::slots},
        storage,
    },
    manifold::timing::Throughput,
    runtime::executor::{create, session},
};

#[derive(Debug)]
pub enum Error<'owner> {
    Create(create::Kind<'owner>),
    Close(domain::Cause<&'owner Infallible>),
}

pub struct Setup {
    config: settings::Config,
}

impl Setup {
    pub fn throughput() -> Self {
        Self {
            config: Config::for_profile::<Throughput>(),
        }
    }
    pub fn for_profile<P: settings::Profile>() -> Self {
        Self {
            config: Config::for_profile::<P>(),
        }
    }
    pub fn try_for_plan<P: settings::Profile, S: settings::Resource>(
        plan: &S,
    ) -> Result<Self, errors::Claim> {
        Ok(Self {
            config: Needs::from_tree(plan)?.resolve::<P>()?,
        })
    }
    pub const fn config(&self) -> settings::Config {
        self.config
    }
    pub fn timers(mut self, timers: slots::Slots) -> Self {
        let schedule = Schedule::fixed::<0, 0>().resolve(demands::Schedule::new(
            self.config.schedule().ready(),
            timers,
        ));
        self.config = self.config.set_schedule(schedule);
        self
    }
    pub fn try_executor(self, attempt: &mut Attempt) -> Result<Executor<()>, create::Error<'_>> {
        Executor::new(attempt, self.config)
    }
    pub fn with_scope<R>(
        self,
        run: impl for<'a, 'd> FnOnce(&mut life::Scope<'a, 'd>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        Driver::new(self.config).with_scope(run, policy, |result| match result {
            Ok(()) => finish(Ok(())),
            Err(scope::Error::Create(error)) => {
                finish(Err(Error::Create(create::Kind::Driver(error.kind()))))
            }
            Err(scope::Error::Close(error)) => finish(Err(Error::Close(error.cause()))),
        })
    }
    pub fn with_driver<R>(
        self,
        run: impl for<'a, 'd> FnOnce(driver::Context<'a, 'd>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        self.with_scope(|scope| run(scope.context()), policy, finish)
    }
    pub fn with_turn<R>(
        self,
        run: impl for<'turn, 'scope, 'd> FnOnce(
            &'turn mut Guard<'scope, 'd>,
            retained::Context<'turn, 'd, 'd>,
        ),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        self.with_scope(
            |scope| {
                scope.with_driver(|context, mut sched| {
                    let owner = retained::raw::Owner::from_driver(context.driver_ref());
                    let mut turn = sched.begin_at(Sample::new(Instant::now()));
                    run(&mut turn, Context::new(context, owner));
                })
            },
            policy,
            finish,
        )
    }
    pub fn with_session<R>(
        self,
        run: impl for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        self.with_direct((), run, policy, finish)
    }
    pub fn with_input<S, R>(
        self,
        value: S,
        run: impl for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, Value<S>>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        self.with_direct(Value::new(value), run, policy, finish)
    }
    fn with_direct<S: storage::Direct, R>(
        self,
        plan: S,
        run: impl for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S>),
        policy: impl recovery::Observe,
        finish: impl for<'owner> FnOnce(Result<(), Error<'owner>>) -> R,
    ) -> R {
        let mut create = Attempt::new();
        let executor = match Executor::new(&mut create, self.config) {
            Ok(executor) => executor,
            Err(error) => return finish(Err(Error::Create(error.kind()))),
        };
        executor
            .with_plan(plan)
            .run(|executor| match executor.enter_with(run, policy) {
                Ok(()) => finish(Ok(())),
                Err(error) => finish(Err(Error::Close(error.cause()))),
            })
    }
}
