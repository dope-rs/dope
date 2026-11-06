use std::{marker, os::fd};

use dope_core::{
    Errno,
    driver::{life::creation, settings},
};
use o3::base::marker::ThreadBound;

use crate::{
    executor::{self, create},
    process::{
        failure::bus,
        lifecycle::{self, Main},
    },
};

pub use crate::process::Shutdown as ReadySource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateError {
    Pair(bus::Create),
    Duplicate(Errno),
}

#[derive(Debug)]
pub struct FireError {
    pub trigger: Trigger,
    pub cause: Errno,
}

#[must_use]
pub struct Pair {
    source: Source,
    trigger: Trigger,
}

#[must_use]
pub struct Source {
    pub(crate) event: (fd::OwnedFd, Main),
}

impl fd::AsFd for Source {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        fd::AsFd::as_fd(&self.event.0)
    }
}

#[must_use]
#[derive(Debug)]
pub struct Trigger(Main);

type Invariant<Q> = marker::PhantomData<(fn(Q) -> Q, ThreadBound)>;

#[must_use]
pub struct Requested<Q = Source>(Invariant<Q>);

impl Pair {
    pub fn new() -> Result<Self, CreateError> {
        let (main, event) = lifecycle::pair().map_err(CreateError::Pair)?;
        let guard = main.duplicate().map_err(CreateError::Duplicate)?;
        Ok(Self {
            source: Source {
                event: (event, guard),
            },
            trigger: Trigger(main),
        })
    }

    pub fn split(self) -> (Source, Trigger) {
        (self.source, self.trigger)
    }

    pub fn run<R>(self, execute: impl FnOnce(Source) -> R) -> R {
        execute(self.source)
    }
}

impl executor::Build for Source {
    type Shutdown = Source;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<executor::Executor<(), Self::Shutdown>, create::Error<'_>> {
        executor::Executor::new(attempt, config)?
            .shutdown(self)
            .map_err(create::Error::from)
    }
}

impl executor::Startup for Source {
    fn installed(&self) -> Result<(), executor::InstallError> {
        Ok(())
    }
}

impl Trigger {
    pub fn fire(self) -> Result<(), FireError> {
        match self.0.request() {
            Ok(()) => Ok(()),
            Err(cause) => Err(FireError {
                trigger: self,
                cause,
            }),
        }
    }
}

impl<Q> Requested<Q> {
    pub(crate) const fn new() -> Self {
        Self(marker::PhantomData)
    }
}

const _: () = assert!(std::mem::size_of::<Requested>() == 0);
