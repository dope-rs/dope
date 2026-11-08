use std::time;

use dope_core::{
    driver,
    driver::{result::poll, schedule, schedule::clock},
};
use drive::turn;
use turn::{modes, scope::Core};

use crate::executor::{self, drive, drive::clock::Time};

#[must_use]
pub(crate) struct Idle<'turn, 'd, M> {
    pub(super) core: Core<'turn, 'd, M>,
}

impl<'turn, 'd, M> Idle<'turn, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(super) const fn new(core: Core<'turn, 'd, M>) -> Self {
        Self { core }
    }

    pub(crate) fn sync(mut self) -> Result<poll::Commit, poll::Error> {
        let work = self.core.turn.reactor();
        driver::Context::commit(&mut self.core.driver, work)
    }

    pub(crate) fn wait(
        mut self,
        progress: schedule::Progress<'d>,
        source: impl Time,
    ) -> Result<(), poll::Error> {
        let span = if matches!(progress, schedule::Progress::Runnable) || !self.is_quiet() {
            Some(time::Duration::ZERO)
        } else {
            match progress {
                schedule::Progress::Waiting(wait) => wait
                    .deadline()
                    .map(|due| due.into_raw().saturating_duration_since(source.park())),
                _ => None,
            }
        };
        let span = source.limit(span);
        driver::Context::wait(&mut self.core.driver, self.core.turn.reactor(), span)
    }

    pub(crate) fn is_quiet(&self) -> bool {
        self.core.is_quiet()
    }

    pub(crate) fn now(&self) -> clock::Moment<'d> {
        self.core.driver.driver_ref().clock().now()
    }
}

impl<'d> Idle<'_, 'd, modes::Boot> {
    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        self.core.timer().reduce(self.core.maintenance())
    }
}

impl<'app, 'd: 'app, D, S> Idle<'_, 'd, modes::App<'_, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(crate) fn progress(&self) -> schedule::Progress<'d> {
        self.core.timer().reduce(self.core.mode.view.progress(
            self.core.mode.token,
            self.core.driver.regions(),
            self.core.driver.driver_ref().clock(),
        ))
    }

    pub(crate) fn is_fixed(&self) -> bool {
        self.is_quiet()
            && match self.progress().reduce(self.core.maintenance()) {
                schedule::Progress::Runnable => false,
                schedule::Progress::Waiting(wait) => {
                    wait.deadline().is_none_or(|due| due > self.now())
                }
                schedule::Progress::Quiescent => true,
            }
    }

    pub(crate) fn shutdown(&self) -> schedule::Progress<'d> {
        self.core.shutdown()
    }

    pub(crate) fn drain(&self) -> schedule::Progress<'d>
    where
        D: executor::raw::Drain<'d>,
    {
        self.core
            .mode
            .view
            .draining(self.core.mode.token, &self.core.driver.regions().0)
    }
}
