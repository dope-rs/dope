use std::{env, io, process, time};

use dope_core::platform::wake;

use crate::harness::scenario::{native, run};

pub mod deadline;
mod state;

pub struct Deadline<'scope> {
    cancel: Option<wake::Notify>,
    join: &'scope mut dyn FnMut() -> Result<Result<(), wake::WaitError>, run::Join>,
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
        let (wait, cancel) = wake::Ends::blocking()
            .map_err(deadline::Error::Create)?
            .split();
        run::scope(
            run::Entry(move || {
                match wait.wait_timeout(deadline.saturating_duration_since(time::Instant::now())) {
                    Err(wake::WaitError::Timeout { .. }) => native::terminate(),
                    result => result,
                }
            }),
            |join| {
                let mut watchdog = Deadline {
                    cancel: Some(cancel),
                    join,
                };
                let result = run(&mut watchdog);
                watchdog.disarm().map(|()| result)
            },
        )
        .map_err(deadline::Error::Start)?
    }

    /// Cancels and joins this watchdog once, before returning to its caller.
    pub fn disarm(&mut self) -> Result<(), deadline::Error> {
        let Some(cancel) = self.cancel.take() else {
            return Ok(());
        };
        let notified = cancel.notify();
        drop(cancel);
        match (notified, (self.join)()) {
            (cancel, Err(cause)) => Err(deadline::Error::Join {
                cancel: cancel.err(),
                cause,
            }),
            (Ok(()), Ok(Ok(()))) => Ok(()),
            (Err(cancel), Ok(Ok(()))) => Err(deadline::Error::Cancel(cancel)),
            (Ok(()), Ok(Err(wait))) => Err(deadline::Error::Wait(wait)),
            (Err(cancel), Ok(Err(wait))) => Err(deadline::Error::Both { cancel, wait }),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Limit(io::Error),
    Signal(io::Error),
    Both { limit: io::Error, signal: io::Error },
}

/// Native resource operations supplied by the consuming platform fixture.
pub trait Resource {
    type Limits;
    type Handler: Copy;

    fn limits() -> io::Result<Self::Limits>;
    fn soft(limits: &Self::Limits, value: u64) -> io::Result<()>;
    fn restore(limits: &Self::Limits) -> io::Result<()>;
    fn ignore() -> io::Result<Self::Handler>;
    fn signal(handler: Self::Handler) -> io::Result<()>;
}

/// One integration test run in its own child process.
#[derive(Debug)]
pub struct Test<'a> {
    pub name: &'a str,
}

impl Test<'_> {
    pub fn try_run(self, run: impl FnOnce()) -> io::Result<Option<process::Output>> {
        const FLAG: &str = "O3_TEST_CHILD";
        if env::var_os(FLAG).as_deref() == Some(std::ffi::OsStr::new(self.name)) {
            run();
            return Ok(None);
        }
        process::Command::new(env::current_exe()?)
            .arg(self.name)
            .arg("--exact")
            .env(FLAG, self.name)
            .output()
            .map(Some)
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
