use std::{convert, num::NonZero, os::fd};

use dope_core::{
    driver::{
        life,
        life::{creation, domain, recovery},
        settings, storage,
    },
    platform::wake,
    random,
};
use o3::buffer;

use crate::process::failure::bus;

pub mod boot;
pub mod create;
pub mod drain;
pub mod drive;
pub mod raw;
pub mod root;
pub mod session;
#[doc(hidden)]
pub mod work;
/// Runs once inside an executor generation with its exact storage family.
pub trait Op<S: storage::Plan, Q>: Sized {
    type Output;

    fn run<'scope, 'life, 'p, 'd>(
        self,
        scope: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: 'd;
}

#[repr(transparent)]
struct Scoped<F>(F);

impl<S, Q, F, R> Op<S, Q> for Scoped<F>
where
    S: storage::Plan,
    F: for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R,
{
    type Output = R;

    fn run<'scope, 'life, 'p, 'd>(
        self,
        scope: session::Scope<'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: 'd,
    {
        (self.0)(scope)
    }
}

struct Entry<O>(O);

impl<S, Q, O> life::Operation<S, Q> for Entry<O>
where
    S: storage::Plan,
    O: Op<S, Q>,
{
    type Output = O::Output;

    fn run<'life, 'p, 'd>(
        self,
        scope: life::Scope<'life, 'd>,
        store: storage::Loan<'life, 'p, 'd, S>,
        source: &'life Q,
    ) -> Self::Output
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
        Q: 'd,
    {
        let mut core = session::Core::new(scope, source);
        let scope = session::Scope::new(store, &mut core);
        Op::run(self.0, scope)
    }
}

/// Compile-time behavior selected by an executor's shutdown source after its
/// pinned application is installed.
pub trait Startup: Sized {
    /// Publishes installation readiness or preserves its exact notification failure.
    fn installed(&self) -> Result<(), InstallError>;
}

impl Startup for () {
    fn installed(&self) -> Result<(), InstallError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallError {
    Claim(settings::errors::Claim),
    Coordinators { count: NonZero<usize>, limit: usize },
    CoordinatorCountOverflow,
    Notify(wake::NotifyError),
    Process(bus::Fault),
}

impl From<settings::errors::Claim> for InstallError {
    fn from(error: settings::errors::Claim) -> Self {
        Self::Claim(error)
    }
}

impl From<wake::NotifyError> for InstallError {
    fn from(error: wake::NotifyError) -> Self {
        Self::Notify(error)
    }
}

impl From<bus::Fault> for InstallError {
    fn from(error: bus::Fault) -> Self {
        Self::Process(error)
    }
}

pub struct Executor<S = (), Q = ()> {
    storage: S,
    generation: domain::Generation<Q>,
    teardown: domain::Teardown,
}

impl Executor<(), ()> {
    pub fn new(
        attempt: &mut creation::Attempt,
        cfg: settings::Config,
    ) -> Result<Self, create::Error<'_>> {
        let driver = attempt.try_driver(cfg)?;
        Ok(Self {
            storage: (),
            generation: domain::Generation::new(driver),
            teardown: domain::Teardown::default(),
        })
    }

    pub(crate) fn with_seed(
        attempt: &mut creation::Attempt,
        cfg: settings::Config,
        seed: random::Seed,
    ) -> Result<Self, create::Error<'_>> {
        let driver = attempt.try_seed(cfg, seed)?;
        Ok(Self {
            storage: (),
            generation: domain::Generation::new(driver),
            teardown: domain::Teardown::default(),
        })
    }
}

impl<S, Q> Executor<S, Q> {
    pub fn with_storage<T>(self, value: T) -> Executor<storage::Value<T>, Q> {
        Executor {
            storage: storage::Value::new(value),
            generation: self.generation,
            teardown: self.teardown,
        }
    }

    pub fn with_plan<T: storage::Plan>(self, plan: T) -> Executor<T, Q> {
        Executor {
            storage: plan,
            generation: self.generation,
            teardown: self.teardown,
        }
    }
}

pub trait Build: Sized {
    type Shutdown: Startup;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<Executor<(), Self::Shutdown>, create::Error<'_>>;
}

impl Build for () {
    type Shutdown = ();

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<Executor<(), Self::Shutdown>, create::Error<'_>> {
        Executor::new(attempt, config)
    }
}

impl<S> Executor<S, ()> {
    /// Installs and owns a process-local shutdown source for this driver domain.
    pub fn shutdown<Q: fd::AsFd>(self, source: Q) -> Result<Executor<S, Q>, domain::Register> {
        let Self {
            storage: store,
            generation: epoch,
            teardown,
        } = self;
        Ok(Executor {
            storage: store,
            generation: epoch.fd(source)?,
            teardown,
        })
    }
}

type ExecResult<'owner, S, O, Q> = Result<
    <O as Op<S, Q>>::Output,
    domain::Error<
        'owner,
        domain::Rejected<<S as storage::Build>::Error, O>,
        <O as Op<S, Q>>::Output,
        Q,
    >,
>;
type CreateResult<'owner, S, F, R, Q> =
    Result<R, domain::Error<'owner, domain::Rejected<<S as storage::Create>::Error, F>, R, Q>>;

/// One execution borrows its exact persistent native retirement owner.
pub struct Execution<'owner, S, Q> {
    storage: S,
    domain: domain::Session<'owner, Q>,
}

impl<S, Q> Executor<S, Q> {
    pub fn run<R>(self, run: impl for<'owner> FnOnce(Execution<'owner, S, Q>) -> R) -> R {
        let Self {
            storage,
            generation: epoch,
            mut teardown,
        } = self;
        let session = epoch.start(&mut teardown);
        run(Execution {
            storage,
            domain: session,
        })
    }
}

impl<'owner, S: storage::Build, Q: Startup> Execution<'owner, S, Q> {
    pub fn try_enter<O>(self, op: O, report: impl recovery::Observe) -> ExecResult<'owner, S, O, Q>
    where
        O: Op<S, Q>,
    {
        let Self {
            storage: store,
            domain,
        } = self;
        domain
            .try_enter(store, Entry(op), report)
            .map_err(|error| error.map_storage(|rejected| rejected.map_op(|entry| entry.0)))
    }
}

impl<'owner, S: storage::Create, Q: Startup> Execution<'owner, S, Q> {
    pub fn try_create<F, R>(
        self,
        op: F,
        report: impl recovery::Observe,
    ) -> CreateResult<'owner, S, F, R, Q>
    where
        F: for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R,
    {
        self.try_scope(Scoped(op), report)
            .map_err(|error| error.map_storage(|rejected| rejected.map_op(|entry| entry.0)))
    }

    pub(crate) fn try_scope<O>(
        self,
        op: O,
        report: impl recovery::Observe,
    ) -> CreateResult<'owner, S, O, O::Output, Q>
    where
        O: Op<S, Q>,
    {
        let Self {
            storage: store,
            domain,
        } = self;
        domain
            .try_create(store, Entry(op), report)
            .map_err(|error| error.map_storage(|rejected| rejected.map_op(|entry| entry.0)))
    }
}

impl<'owner, S, Q> Execution<'owner, S, Q>
where
    S: storage::Direct,
    Q: Startup,
{
    pub fn enter<O>(
        self,
        op: O,
        report: impl recovery::Observe,
    ) -> Result<O::Output, domain::Error<'owner, convert::Infallible, O::Output, Q>>
    where
        O: Op<S, Q>,
    {
        let Self {
            storage: store,
            domain,
        } = self;
        domain.enter(store, Entry(op), report)
    }

    pub fn enter_with<R>(
        self,
        op: impl for<'scope, 'life, 'p, 'd> FnOnce(session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> R,
        report: impl recovery::Observe,
    ) -> Result<R, domain::Error<'owner, convert::Infallible, R, Q>> {
        self.enter(Scoped(op), report)
    }
}

impl<'owner, A: buffer::raw::Arenas, Q: Startup> Execution<'owner, storage::Scope<A>, Q> {
    /// Lends exact arena roots from the same generation as the driver.
    pub fn scope<R>(
        self,
        op: impl for<'scope, 'life, 'p, 'd> FnOnce(
            session::Scope<'scope, 'life, 'p, 'd, storage::Scope<A>, Q>,
        ) -> R,
        report: impl recovery::Observe,
    ) -> Result<R, domain::Error<'owner, convert::Infallible, R, Q>> {
        let Self {
            storage: store,
            domain,
        } = self;
        domain.scope(store, Entry(Scoped(op)), report)
    }
}
