use std::{marker, mem, result::Result as StdResult};

use dope_core::driver::{
    settings,
    settings::{claim, errors},
    storage,
};
use o3::mem::slot;

use crate::{
    client,
    executor::{self, drive, session},
    shutdown,
    topology::{self, lending},
};

mod build;
mod error;
mod execute;
mod outcome;
mod permit;
pub mod raw;
mod root;
mod stage;

pub use build::Build;
pub use error::Error;
/// Constructed output or the exact build and execution failure.
pub type Result<'report, E, T> = std::result::Result<T, Error<'report, E, T>>;

pub use execute::Execute;
pub use outcome::Outcome;
pub use permit::Permit;
pub use root::Root;
pub use stage::Stage;

/// Owns one provider in its final storage while dependent storage is prepared.
#[repr(transparent)]
pub struct Held<'held, 'd: 'held, O> {
    owner: slot::Occupied<'held, O>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'held, 'd: 'held, O> Held<'held, 'd, O>
where
    O: lending::Root<'d> + lending::raw::Resume<'d>,
{
    fn new(owner: slot::Occupied<'held, O>) -> Self {
        const { assert!(mem::size_of::<Self>() == mem::size_of::<usize>()) };
        Self {
            owner,
            driver: marker::PhantomData,
        }
    }

    /// Consumes the resident provider after every dependent storage scope exists.
    pub fn with_lent<'report, 'stage, 'scope, 'life, 'p, S, Q, B>(
        self,
        stage: &mut Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        report: &'report mut session::Completion,
        build: B,
    ) -> lending::Result<'report, B::Error, B::Output>
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope,
        S: storage::Plan + 'd,
        Q: executor::Startup,
        B: lending::Execute<'life, 'd, O, Q>,
    {
        session::Lent::new(stage.session.core).held(report, self.owner, build, None)
    }

    /// Keeps provider destruction inside the final dependent storage callback.
    pub fn lend_scope<'report, 'stage, 'scope, 'life, 'p, S, Q, E, R>(
        self,
        stage: &mut Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
        report: &'report mut session::Completion,
        build: impl for<'issued> FnOnce(
            lending::Issued<'issued, 'life, 'd, O, Q>,
        ) -> lending::Outcome<'issued, 'life, 'd, O, Q, E, R>,
    ) -> lending::Result<'report, E, R>
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope,
        S: storage::Plan + 'd,
        Q: executor::Startup,
    {
        session::Lent::new(stage.session.core).scope_held(report, self.owner, build)
    }
}

/// Couples one client with the sole execution authority for its provider.
pub struct Issued<'issued, 'life: 'issued, 'd: 'life, O, Q>
where
    O: lending::Root<'d>,
    O::Provider: 'issued,
{
    client: <O::Provider as client::Provider<'d>>::Client<'issued>,
    permit: lending::Permit<'issued, 'life, 'd, O, Q>,
}

impl<'issued, 'life: 'issued, 'd: 'life, O, Q> Issued<'issued, 'life, 'd, O, Q>
where
    O: lending::Root<'d>,
    O::Provider: 'issued,
{
    pub(crate) fn new(
        client: <O::Provider as client::Provider<'d>>::Client<'issued>,
        permit: lending::Permit<'issued, 'life, 'd, O, Q>,
    ) -> Self {
        Self { client, permit }
    }

    pub fn into_parts(
        self,
    ) -> (
        <O::Provider as client::Provider<'d>>::Client<'issued>,
        lending::Permit<'issued, 'life, 'd, O, Q>,
    ) {
        (self.client, self.permit)
    }
}

/// Executes the constructed application with caller-controlled protocol time.
pub trait ExecuteAt<'life, 'd: 'life, O, Q>: lending::Build<'d, O>
where
    O: lending::Root<'d> + executor::raw::App<'d>,
{
    type Output;

    fn run<'app, 'run>(
        data: Self::Data,
        app: session::Manual<'run, 'life, 'd, lending::raw::Bound<'app, 'd, O, Self::App<'app>>, Q>,
    ) -> Self::Output
    where
        Self: 'app,
        O: 'app,
        O::Provider: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run;
}

/// Consumes an application while its installation storage is borrowed.
pub trait Next<C, A>: Sized {
    type Output;

    fn run(self, app: A, context: &mut C) -> Self::Output;
}

impl<C, A, F, R> Next<C, A> for F
where
    F: FnOnce(A, &mut C) -> R,
{
    type Output = R;

    fn run(self, app: A, context: &mut C) -> R {
        self(app, context)
    }
}

/// Keeps constructor-owned storage alive through the final application consumer.
pub trait Installer<C, N>: Sized {
    type Output;
    type Error;

    fn install(self, context: &mut C, next: N) -> StdResult<Self::Output, Self::Error>;
}

/// Drives the installed application while every installation owner is borrowed.
pub struct Run<'report>(pub &'report mut session::Completion);

/// Drives and retires an application whose shutdown proof stays in this scope.
#[derive(Debug)]
pub struct Clean<'report>(pub &'report mut session::Completion);

impl<'report, 'stage, 'scope, 'life, 'p, 'd, S, Q, A>
    lending::Next<lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>, A> for Clean<'report>
where
    S: storage::Plan,
    Q: executor::Startup,
    A: executor::raw::App<'d>,
{
    type Output = StdResult<(), session::Error<'report, StdResult<(), drive::Error>>>;

    fn run(
        self,
        app: A,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output {
        stage.run(self.0, app)?.map_err(session::Error::Drive)
    }
}

impl<'report, 'stage, 'scope, 'life, 'p, 'd, S, Q, A>
    lending::Next<lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>, A> for Run<'report>
where
    S: storage::Plan,
    Q: executor::Startup,
    A: executor::raw::App<'d>,
{
    type Output = StdResult<
        shutdown::Requested<Q>,
        session::Error<'report, StdResult<shutdown::Requested<Q>, drive::Error>>,
    >;

    fn run(
        self,
        app: A,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output {
        stage.request(self.0, app)?.map_err(session::Error::Drive)
    }
}

const _: () = assert!(std::mem::size_of::<Run<'_>>() == std::mem::size_of::<usize>());

/// Installs a provider and its dependent operation inside one valid driver scope.
pub trait ComposeFrom<S: storage::Plan, T, Q: executor::Startup>: Sized {
    type Output;
    type Error;

    fn compose<'scope, 'life, 'p, 'd>(
        self,
        tail: T,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> StdResult<Self::Output, Self::Error>
    where
        'life: 'scope,
        'p: 'life,
        'd: 'p,
        Self: 'd,
        T: 'd,
        S: 'd,
        Q: 'd;
}

/// Retains a dependent resource plan and its operation through provider installation.
pub struct Composition<T, F> {
    value: T,
    run: F,
}

impl<T, F> Composition<T, F> {
    pub const fn new(value: T, run: F) -> Self {
        Self { value, run }
    }
}

impl<T: settings::Resource, F> settings::Resource for Composition<T, F> {
    fn include_in(&self, needs: claim::Needs) -> StdResult<claim::Needs, errors::Claim> {
        self.value.include_in(needs)
    }
}

impl<'scope, 'life, 'p, 'd, S, Q, O, T, F, R> Compose<'scope, 'life, 'p, 'd, S, Q, O>
    for Composition<T, F>
where
    S: storage::Plan,
    O: lending::Root<'d>,
    F: for<'stage> FnOnce(T, O, &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>) -> R,
{
    type Output = R;

    fn compose(self, owner: O, stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>) -> R {
        (self.run)(self.value, owner, stage)
    }
}

/// Prepares dependent storage before the executor issues a provider client.
pub trait Compose<'scope, 'life, 'p, 'd, S: storage::Plan, Q, O>
where
    O: lending::Root<'d>,
{
    type Output;

    fn compose(
        self,
        owner: O,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Self::Output;
}

impl<'report> Run<'report> {
    /// Runs the installer within its exact established staging lifetime.
    pub fn install<'stage, 'scope, 'life, 'p, 'd, S, I, Q, E>(
        self,
        source: I,
        stage: &mut Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> StdResult<shutdown::Requested<Q>, topology::Failure<'report, E, shutdown::Requested<Q>>>
    where
        S: storage::Plan,
        Q: executor::Startup,
        I: Installer<
                Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>,
                Self,
                Output = StdResult<
                    shutdown::Requested<Q>,
                    session::Error<'report, StdResult<shutdown::Requested<Q>, drive::Error>>,
                >,
                Error = E,
            >,
    {
        source
            .install(stage, self)
            .map_err(topology::Failure::Install)?
            .map_err(topology::Failure::Run)
    }
}
