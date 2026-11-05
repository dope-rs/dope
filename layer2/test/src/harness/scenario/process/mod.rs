use std::{ffi::OsStr, path::Path, process, time};

use dope_core::Errno;
use dope_runtime::process::lifecycle::{Main, Outcome, WaitError};

use crate::harness::scenario::{native, run};

pub mod deadline;
mod state;

pub const CHILD: &str = "O3_TEST_CHILD";

pub struct Deadline<'scope> {
    worker: Option<(Main, run::Join<'scope, Outcome<(), WaitError>>)>,
}

impl Deadline<'_> {
    /// Keeps the timeout worker owned until the callback has returned and cancelled it.
    pub fn with<R>(
        duration: time::Duration,
        run: impl for<'scope> FnOnce(&mut Deadline<'scope>) -> R,
    ) -> Result<R, deadline::Error> {
        let deadline = time::Instant::now()
            .checked_add(duration)
            .ok_or(deadline::Error::Overflow { duration })?;
        let (cancel, worker) = Main::try_pair().map_err(deadline::Error::Create)?;
        run::scope(
            run::Entry(move || {
                worker.run(|source| match source.control().wait_stop_until(deadline) {
                    Err(WaitError::Timeout { .. }) => native::terminate(),
                    result => result,
                })
            }),
            |join| {
                let mut watchdog = Deadline {
                    worker: Some((cancel, join)),
                };
                let result = run(&mut watchdog);
                watchdog.disarm().map(|()| result)
            },
        )
        .map_err(deadline::Error::Start)?
    }

    /// Cancels and joins this watchdog once, before returning to its caller.
    pub fn disarm(&mut self) -> Result<(), deadline::Error> {
        let Some((cancel, join)) = self.worker.take() else {
            return Ok(());
        };
        let stopped = cancel.stop();
        let outcome = join.finish();
        match (stopped, outcome) {
            (Ok(_), Outcome::Done(())) => Ok(()),
            (Err(cancel), Outcome::Done(())) => Err(deadline::Error::Cancel(cancel)),
            (stopped, Outcome::Failed { output, report }) => Err(deadline::Error::Worker {
                cancel: stopped.err(),
                output,
                report,
            }),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Limit(Errno),
    Signal(Errno),
    Both { limit: Errno, signal: Errno },
}

/// Native resource operations supplied by the consuming platform fixture.
pub trait Resource {
    type Limits;
    type Handler: Copy;

    fn limits() -> Result<Self::Limits, Errno>;
    fn soft(limits: &Self::Limits, value: u64) -> Result<(), Errno>;
    fn restore(limits: &Self::Limits) -> Result<(), Errno>;
    fn ignore() -> Result<Self::Handler, Errno>;
    fn signal(handler: Self::Handler) -> Result<(), Errno>;
}

/// One integration test run in its own child process.
#[derive(Debug)]
pub struct Test<'a> {
    pub name: &'a str,
}

impl Test<'_> {
    pub fn run(
        self,
        exe: &Path,
        child: Option<&OsStr>,
        run: impl FnOnce(),
    ) -> Option<process::Command> {
        self.invoke(exe, child, run, false)
    }

    /// Configures direct child execution so libtest cannot replace its termination signal.
    pub fn signal(
        self,
        exe: &Path,
        child: Option<&OsStr>,
        run: impl FnOnce(),
    ) -> Option<process::Command> {
        self.invoke(exe, child, run, true)
    }

    fn invoke(
        self,
        exe: &Path,
        child: Option<&OsStr>,
        run: impl FnOnce(),
        direct: bool,
    ) -> Option<process::Command> {
        if child == Some(OsStr::new(self.name)) {
            run();
            return None;
        }
        let mut child = process::Command::new(exe);
        child
            .args([self.name, "--exact"])
            .env(CHILD, self.name)
            .env_remove("__RUST_TEST_INVOKE");
        if direct {
            child.env("__RUST_TEST_INVOKE", self.name);
        }
        Some(child)
    }
}

/// A process-local file-size ceiling for an isolated test child.
#[must_use = "keep the process resource limit active until the guarded operation ends"]
pub struct Limit<R: Resource, F: FnOnce(Result<(), Error>)>(Option<(state::State<R>, F)>);

impl<R: Resource, F: FnOnce(Result<(), Error>)> Limit<R, F> {
    pub fn try_zero(report: F) -> Result<Self, Error> {
        Ok(Self(Some((state::State::try_zero()?, report))))
    }

    /// Restores now, returning the exact result instead of calling the observer.
    pub fn try_restore(mut self) -> Result<(), Error> {
        self.0
            .take()
            .map_or(Ok(()), |(state, _report)| state.try_reset())
    }
}

impl<R: Resource, F: FnOnce(Result<(), Error>)> Drop for Limit<R, F> {
    fn drop(&mut self) {
        if let Some((state, report)) = self.0.take() {
            report(state.try_reset());
        }
    }
}
