use std::{io, os::fd, pin, task};

use dope_core::driver::{
    self, lifecycle, retained,
    schedule::{self, ready},
    settings, storage,
};

use crate::{random, shutdown};

mod application;
#[doc(hidden)]
pub mod raw;
pub mod session;
mod startup;

pub use application::Application;
pub use startup::Startup;

/// Runtime poll capability tying one exact root and driver to a poll.
/// ```compile_fail,E0308
/// fn retag<'a, 'd: 'a, A, B>(value: dope_runtime::executor::RootContext<'a, 'd, A>) -> dope_runtime::executor::RootContext<'a, 'd, B> { value }
/// ```
/// ```compile_fail,E0624
/// let _ = dope_runtime::executor::RootContext::<()>::new(todo!(), todo!(), todo!());
/// ```
/// ```compile_fail
/// fn escape<'a, 'd: 'a, R>(value: dope_runtime::executor::RootContext<'a, 'd, R>) -> std::pin::Pin<&'d mut R> { value.into_parts().0 }
/// ```
pub struct RootContext<'poll, 'd: 'poll, R> {
    root: pin::Pin<&'poll mut R>,
    wake: ready::Target<'d>,
    work: schedule::Application<'poll, 'd>,
    driver: retained::Context<'poll, 'poll, 'd>,
}

impl<'poll, 'd: 'poll, R> RootContext<'poll, 'd, R> {
    pub(crate) fn new(
        root: pin::Pin<&'poll mut R>,
        wake: ready::Target<'d>,
        work: schedule::Application<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
    ) -> Self {
        Self {
            root,
            wake,
            work,
            driver,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'poll mut R>,
        ready::Target<'d>,
        schedule::Application<'poll, 'd>,
        retained::Context<'poll, 'poll, 'd>,
    ) {
        (self.root, self.wake, self.work, self.driver)
    }
}

/// Transient pinned control-flow root.
/// Its driver authority retains the installed application through terminal
/// completion and quiescence; backends must not retain root-owned fields.
pub trait Root<'d>: Sized {
    type Output;
    fn poll(context: RootContext<'_, 'd, Self>) -> task::Poll<Self::Output>;
}

pub struct Executor<S = (), Q = ()> {
    storage: S,
    domain: lifecycle::Domain<Q>,
    seed: random::Seed,
}

impl Executor<(), ()> {
    pub fn new(cfg: settings::Config) -> io::Result<Self> {
        Self::with_seed(cfg, random::Seed::random()?)
    }

    pub(crate) fn with_seed(cfg: settings::Config, seed: random::Seed) -> io::Result<Self> {
        let driver = driver::Driver::new(cfg)?;
        Ok(Self {
            storage: (),
            domain: lifecycle::Domain::new(driver),
            seed,
        })
    }
}

impl<S, Q> Executor<S, Q> {
    pub fn with_storage<T: 'static>(self, value: T) -> Executor<storage::Value<T>, Q> {
        Executor {
            storage: storage::Value::new(value),
            domain: self.domain,
            seed: self.seed,
        }
    }

    pub fn with_factory<T: storage::Factory>(self, storage: T) -> Executor<T, Q> {
        Executor {
            storage,
            domain: self.domain,
            seed: self.seed,
        }
    }
}

pub trait Factory: Sized {
    type Shutdown: Startup;

    fn executor(self, config: settings::Config) -> io::Result<Executor<(), Self::Shutdown>>;
}

impl<S> Executor<S, ()> {
    /// Installs and owns a process-local shutdown source for this driver domain.
    pub fn with_shutdown(
        self,
        source: shutdown::Source,
    ) -> io::Result<Executor<S, shutdown::Source>> {
        self.with_source(source, |source| fd::AsFd::as_fd(&source.event))
    }

    pub(crate) fn with_source<Q>(
        self,
        source: Q,
        select: impl for<'a> FnOnce(&'a Q) -> fd::BorrowedFd<'a>,
    ) -> io::Result<Executor<S, Q>> {
        let Self {
            storage,
            domain,
            seed,
        } = self;
        let domain = domain.fd(source, select)?;
        Ok(Executor {
            storage,
            domain,
            seed,
        })
    }
}

impl<S: storage::Factory, Q: Startup> Executor<S, Q> {
    pub fn try_enter<R>(
        self,
        f: impl for<'scope, 'd> FnOnce(session::Session<'scope, 'd, S::Output<'d>, Q>) -> R,
    ) -> Result<R, S::Error> {
        let Self {
            storage,
            domain,
            seed,
        } = self;
        let owner = crate::Owner::acquire().into_inner();
        domain.enter(owner, storage, move |scope, storage, source| {
            let mut core = session::Core::new(scope, seed);
            f(session::Session::new(storage, &mut core, source))
        })
    }
}

impl<S, Q> Executor<S, Q>
where
    S: storage::Factory,
    S::Error: storage::Never,
    Q: Startup,
{
    pub fn enter<R>(
        self,
        f: impl for<'scope, 'd> FnOnce(session::Session<'scope, 'd, S::Output<'d>, Q>) -> R,
    ) -> R {
        match self.try_enter(f) {
            Ok(output) => output,
            Err(error) => storage::Never::never(error),
        }
    }
}
