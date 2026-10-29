use std::{cell::Cell, marker, mem, os::fd, time};

use dope_core::{
    driver::{life::creation, settings},
    platform::wake::{CreateError, Ends, Notify, NotifyError, Wait, WaitError},
};
use o3::base::marker::ThreadBound;

use crate::executor::{self, create};

#[derive(Debug)]
pub struct FireError {
    pub trigger: Trigger,
    pub cause: NotifyError,
}

#[must_use]
pub struct Pair {
    source: Source,
    trigger: Trigger,
}

#[must_use]
pub struct Source {
    pub(crate) event: (fd::OwnedFd, Notify),
}

impl fd::AsFd for Source {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        fd::AsFd::as_fd(&self.event.0)
    }
}

#[must_use]
#[derive(Debug)]
pub struct Trigger(Notify);

/// Shutdown source whose application-install edge is observable exactly once.
#[must_use]
pub struct ReadySource {
    shutdown: Source,
    ready: Cell<Option<Notify>>,
}

impl fd::AsFd for ReadySource {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        fd::AsFd::as_fd(&self.shutdown)
    }
}

/// Blocking observer for one application-install edge.
#[must_use]
pub struct Readiness(Wait);

/// Affine shutdown and readiness authorities for a single runtime.
#[must_use]
pub struct ReadyPair {
    source: ReadySource,
    trigger: Trigger,
    readiness: Readiness,
}

type Invariant<Q> = marker::PhantomData<(fn(Q) -> Q, ThreadBound)>;

#[must_use]
pub struct Requested<Q = Source>(Invariant<Q>);

impl Pair {
    pub fn new() -> Result<Self, CreateError> {
        let (event, notify) = Ends::event()?.split();
        let (guard, notify) = notify.fork()?;
        Ok(Self {
            source: Source {
                event: (event, guard),
            },
            trigger: Trigger(notify),
        })
    }

    pub fn split(self) -> (Source, Trigger) {
        (self.source, self.trigger)
    }

    pub fn run<R>(self, execute: impl FnOnce(Source) -> R) -> R {
        execute(self.source)
    }
}

impl ReadyPair {
    pub fn new() -> Result<Self, CreateError> {
        let (stop, trig) = Pair::new()?.split();
        let (ready, notify) = Ends::blocking()?.split();
        Ok(Self {
            source: ReadySource {
                shutdown: stop,
                ready: Cell::new(Some(notify)),
            },
            trigger: trig,
            readiness: Readiness(ready),
        })
    }

    pub fn split(self) -> (ReadySource, Trigger, Readiness) {
        (self.source, self.trigger, self.readiness)
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

impl executor::Build for ReadySource {
    type Shutdown = ReadySource;

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

impl executor::Startup for ReadySource {
    fn installed(&self) -> Result<(), executor::InstallError> {
        let Some(notify) = self.ready.take() else {
            return Ok(());
        };
        match notify.notify() {
            Ok(()) => Ok(()),
            Err(NotifyError::Write(error)) if error.is_pipe() => Ok(()),
            Err(error) => {
                self.ready.set(Some(notify));
                Err(error.into())
            }
        }
    }
}

impl Readiness {
    pub fn wait(self) -> Result<(), WaitError> {
        self.0.wait()
    }

    pub fn wait_timeout(self, limit: time::Duration) -> Result<(), WaitError> {
        self.0.wait_timeout(limit)
    }
}

impl Trigger {
    pub fn fire(self) -> Result<(), FireError> {
        match self.0.notify() {
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
const _: () = assert!(mem::size_of::<Option<Notify>>() == mem::size_of::<fd::OwnedFd>());
