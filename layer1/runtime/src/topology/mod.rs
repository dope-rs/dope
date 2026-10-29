//! Typed startup composition for one exact generative driver domain.
use std::{mem, pin};

use dope_core::{
    driver,
    driver::{
        life::domain::{Closing, Error, Rejected},
        settings,
        settings::{claim, errors},
        storage,
    },
};

use crate::{
    executor::{self, create, drive, session},
    random::{self, HashState},
};

pub mod app;
mod domain;
pub mod error;
pub mod lending;
pub mod ops;
mod prepared;
pub mod recipe;
mod worker;

pub use domain::Domain;
pub use prepared::Prepared;
pub use worker::Worker;

/// Pinned storage available while constructing a yielding startup root.
///
/// This capability cannot submit I/O, allocate ready slots, or create tasks.
#[repr(transparent)]
pub struct Boot<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q> {
    session: &'stage session::Scope<'scope, 'life, 'p, 'd, S, Q>,
}

impl<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q>
    Boot<'stage, 'scope, 'life, 'p, 'd, S, Q>
{
    fn new(session: &'stage session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> Self {
        Self { session }
    }

    /// Borrows storage while preserving its pinned address.
    pub fn pinned(&self) -> pin::Pin<&'life S::Output<'p, 'd>> {
        self.session.pinned()
    }

    /// Storage branded by the exact prepared driver domain.
    pub fn storage(&self) -> &'life S::Output<'p, 'd> {
        self.session.storage()
    }
}

/// Exclusive installation authority issued by [`Prepared`].
///
/// Its private session exposes branded resources only while [`Worker::install`]
/// consumes the exact claim-producing value; the wrapper then disappears.
pub struct Stage<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q> {
    session: &'stage mut session::Scope<'scope, 'life, 'p, 'd, S, Q>,
}

impl<'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S: storage::Plan + 'd, Q>
    Stage<'stage, 'scope, 'life, 'p, 'd, S, Q>
{
    fn new(session: &'stage mut session::Scope<'scope, 'life, 'p, 'd, S, Q>) -> Self {
        Self { session }
    }

    /// Storage branded by the exact prepared driver domain.
    pub fn storage(&self) -> &'life S::Output<'p, 'd> {
        self.session.storage()
    }

    /// Issues the domain's keyed hash capability without exposing the raw
    /// executor session.
    pub fn hash_state(&mut self, domain: random::Domain) -> HashState<'d> {
        self.session.hash_state(domain)
    }

    /// Borrows the exact prepared driver's staging context.
    pub fn driver(&mut self) -> driver::Context<'_, 'd> {
        self.session.driver()
    }
}

/// Stages one rollback-safe application from exact driver storage.
pub trait Installer<S: storage::Plan>: Sized {
    type App<'life, 'p, 'd>: executor::raw::App<'d>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
    type Error;

    fn install<'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Result<Self::App<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: executor::Startup;
}

/// Driver entry failure retaining the rejected operation or its closing owner.
pub enum EnterError<'owner, S, O, T, Q = ()> {
    Runtime(Rejected<create::Error<'owner>, O>),
    Storage(Rejected<S, O>),
    Close(Closing<'owner, Rejected<S, O>, T, Q>),
}

impl<'owner, S, O, T, Q> EnterError<'owner, S, O, T, Q> {
    /// Borrows completed application output while this error retains its teardown owner.
    pub fn output(&self) -> Option<&T> {
        match self {
            Self::Runtime(..) | Self::Storage(..) => None,
            Self::Close(state) => state.result().as_ref().ok(),
        }
    }

    /// Borrows every failure while preserving output, operation, and retry ownership.
    pub fn cause(&self) -> error::Cause<'_, S> {
        match self {
            Self::Runtime(error) => error::Cause::Runtime(error.cause.kind()),
            Self::Storage(error) => error::Cause::Storage(&error.cause),
            Self::Close(state) => error::Cause::closing(state),
        }
    }

    pub fn kind(&self) -> error::Kind<'_, S, &T> {
        match self {
            Self::Runtime(error) => error::Kind::Runtime(error.cause.kind()),
            Self::Storage(error) => error::Kind::Storage(&error.cause),
            Self::Close(state) => error::Kind::closing(state),
        }
    }
}

#[derive(Debug)]
pub enum InstallError<E> {
    Drive(drive::Error),
    Install(E),
}

#[derive(Debug)]
pub enum Failure<'report, I, T> {
    Run(session::Error<'report, Result<T, drive::Error>>),
    Install(I),
}

impl<I, T> Failure<'_, I, T> {
    /// Borrows every failure without moving or formatting successful output.
    pub fn cause(&self) -> error::Failure<'_, I> {
        match self {
            Self::Run(error) => error::Failure::Run {
                cause: error.cause(),
                output: match error.output() {
                    Some(Err(error)) => Some(error),
                    Some(Ok(_)) | None => None,
                },
            },
            Self::Install(error) => error::Failure::Install(error),
        }
    }
}

pub enum RunError<'owner, S, I, O, T, Q = ()> {
    Create(Rejected<create::Error<'owner>, O>),
    Storage(Rejected<S, O>),
    Failure(Failure<'owner, I, T>),
    Close(ClosingRun<'owner, S, I, O, T, Q>),
}

impl<'owner, S, I, O, T, Q> RunError<'owner, S, I, O, T, Q> {
    /// Borrows every failure while preserving output, operation, and retry ownership.
    pub fn cause(&self) -> error::Run<'_, S, I> {
        match self {
            Self::Create(error) => error::Run::Runtime(error.cause.kind()),
            Self::Storage(error) => error::Run::Storage(&error.cause),
            Self::Failure(error) => error::Run::Failure(error.cause()),
            Self::Close(state) => error::Run::Close {
                result: match state.result() {
                    Ok(Ok(_)) => Ok(Ok(())),
                    Ok(Err(error)) => Ok(Err(error.cause())),
                    Err(error) => Err(&error.cause),
                },
                failure: state.failure(),
            },
        }
    }

    pub fn kind(&self) -> RunKind<'_, 'owner, S, I, T> {
        match self {
            Self::Create(error) => error::Kind::Runtime(error.cause.kind()),
            Self::Storage(error) => error::Kind::Storage(&error.cause),
            Self::Failure(error) => error::Kind::Run(error),
            Self::Close(state) => error::Kind::closing(state),
        }
    }
}

impl<'owner, S, O, T, Q> From<Error<'owner, Rejected<S, O>, T, Q>>
    for EnterError<'owner, S, O, T, Q>
{
    fn from(error: Error<'owner, Rejected<S, O>, T, Q>) -> Self {
        match error {
            Error::Storage(source) => Self::Storage(source),
            Error::Close(state) => Self::Close(state),
        }
    }
}

impl<'owner, S, I, O, T, Q> From<EnterError<'owner, S, O, Result<T, Failure<'owner, I, T>>, Q>>
    for RunError<'owner, S, I, O, T, Q>
{
    fn from(error: EnterError<'owner, S, O, Result<T, Failure<'owner, I, T>>, Q>) -> Self {
        match error {
            EnterError::Runtime(source) => Self::Create(source),
            EnterError::Storage(source) => Self::Storage(source),
            EnterError::Close(state) => Self::Close(state),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Boot<'static, 'static, 'static, 'static, 'static, (), ()>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::align_of::<Boot<'static, 'static, 'static, 'static, 'static, (), ()>>()
            == mem::align_of::<usize>()
    );
    assert!(
        mem::size_of::<Stage<'static, 'static, 'static, 'static, 'static, (), ()>>()
            == mem::size_of::<&mut session::Scope<'static, 'static, 'static, 'static, (), ()>>()
    );
};

/// Unresolved storage and installer retained as one startup input.
///
/// Resolution consumes both values and retains the claimed installer in the
/// resulting [`Prepared`], so installation uses those exact resource owners.
#[must_use = "a topology plan must be resolved before it can enter a driver"]
#[repr(transparent)]
pub struct Plan<S, I>((S, I));

impl<S, I> Plan<S, I> {
    pub const fn new(storage: S, install: I) -> Self {
        Self((storage, install))
    }

    /// Transfers both resource owners into a larger topology.
    pub fn into_parts(self) -> (S, I) {
        self.0
    }

    pub fn for_profile<P>(self) -> Result<Prepared<S, I>, errors::Claim>
    where
        P: settings::Profile,
        S: storage::Plan + settings::Resource,
        I: settings::Resource,
    {
        let (storage, install) = self.0;
        let needs = claim::Needs::from_tree(&(&storage, &install))?;
        let layout = needs.resolve::<P>()?;
        Ok(Prepared::new(layout, storage, install))
    }

    pub fn with_config(self, baseline: settings::Config) -> Result<Prepared<S, I>, errors::Claim>
    where
        S: storage::Plan + settings::Resource,
        I: settings::Resource,
    {
        let (storage, install) = self.0;
        let needs = claim::Needs::from_tree(&(&storage, &install))?;
        let layout = needs.apply_to(baseline)?;
        Ok(Prepared::new(layout, storage, install))
    }

    pub fn for_receive<P>(self) -> Result<Prepared<S, I>, errors::Claim>
    where
        P: settings::Profile,
        S: storage::Plan + settings::Resource,
        I: settings::Resource,
    {
        let (storage, install) = self.0;
        let needs = claim::Needs::from_tree(&(&storage, &install))?;
        let layout = needs.exact::<P>()?;
        Ok(Prepared::new(layout, storage, install))
    }

    pub fn for_install<P>(self) -> Result<Prepared<S, I>, errors::Claim>
    where
        P: settings::Profile,
        S: storage::Claimless,
        I: settings::Resource,
    {
        let (storage, install) = self.0;
        let needs = claim::Needs::from_tree(&install)?;
        let layout = needs.resolve::<P>()?;
        Ok(Prepared::new(layout, storage, install))
    }

    pub fn with_install(self, baseline: settings::Config) -> Result<Prepared<S, I>, errors::Claim>
    where
        S: storage::Claimless,
        I: settings::Resource,
    {
        let (storage, install) = self.0;
        let needs = claim::Needs::from_tree(&install)?;
        let layout = needs.apply_to(baseline)?;
        Ok(Prepared::new(layout, storage, install))
    }
}

const _: () = {
    assert!(mem::size_of::<Plan<usize, u8>>() == mem::size_of::<(usize, u8)>());
    assert!(mem::align_of::<Plan<usize, u8>>() == mem::align_of::<(usize, u8)>());
    assert!(mem::needs_drop::<Plan<String, ()>>() == mem::needs_drop::<String>());
};

type ClosingRun<'owner, S, I, O, T, Q> =
    Closing<'owner, Rejected<S, O>, Result<T, Failure<'owner, I, T>>, Q>;
type RunKind<'a, 'owner, S, I, T> =
    error::Kind<'a, S, &'a Result<T, Failure<'owner, I, T>>, Failure<'owner, I, T>>;
