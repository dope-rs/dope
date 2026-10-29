use std::{mem, time};

use dope_core::{
    driver,
    driver::{
        life,
        result::poll::fault,
        schedule,
        schedule::{clock, ready, state},
    },
    io::Event,
};
use drive::turn;
use o3::base::cell::brand;
use state::scope;
use turn::{
    modes,
    ready::{Ready, RootReady},
    routes,
};

use crate::executor::{self, drive, drive::clock::Time};

pub(crate) struct Scope<'run, 'driver, 'app, 'd: 'app, D: executor::raw::Layout<'d>, S> {
    driver: &'run mut driver::Context<'driver, 'd>,
    view: executor::raw::View<'app, 'd, D, S, D::ParkPlan>,
    token: &'run mut brand::Token<brand::Identity<'d>>,
    schedule: scope::Scope<'driver, 'd>,
    pending: &'run mut Option<Event<'d>>,
}

const _: () = {
    assert!(mem::size_of::<Boot<'_, '_, '_>>() == 3 * mem::size_of::<usize>());
    assert!(mem::align_of::<Boot<'_, '_, '_>>() == mem::align_of::<usize>());
};

/// Exclusive startup authority admitted without unresolved application completions.
pub(crate) struct Boot<'run, 'driver, 'd> {
    driver: &'run mut driver::Context<'driver, 'd>,
    schedule: scope::Scope<'driver, 'd>,
    pending: &'run mut Option<Event<'d>>,
    mode: modes::Boot,
}

pub(crate) struct Core<'turn, 'd, M> {
    pub(super) driver: driver::Context<'turn, 'd>,
    pub(super) turn: scope::Guard<'turn, 'd>,
    pub(super) pending: &'turn mut Option<Event<'d>>,
    pub(super) mode: M,
}

pub(crate) trait Roots<'d> {
    type Mode<'turn>: modes::Mode<'d>
    where
        Self: 'turn;

    fn driver_ref(&self) -> driver::Reference<'d>;

    fn next_root<'turn>(
        &'turn mut self,
        root: &ready::Slot<'d, routes::Wake>,
        tick: clock::Sample,
    ) -> Result<RootReady<'turn, 'd, Self::Mode<'turn>>, fault::Dispatch>;
}

impl<'run, 'driver, 'app, 'd: 'app, D, S> Scope<'run, 'driver, 'app, 'd, D, S>
where
    D: executor::raw::App<'d>,
{
    pub(crate) const fn new(
        driver: &'run mut driver::Context<'driver, 'd>,
        view: executor::raw::View<'app, 'd, D, S, D::ParkPlan>,
        token: &'run mut brand::Token<brand::Identity<'d>>,
        schedule: scope::Scope<'driver, 'd>,
        pending: &'run mut Option<Event<'d>>,
    ) -> Self {
        Self {
            driver,
            view,
            token,
            schedule,
            pending,
        }
    }

    pub(crate) fn next_app(
        &mut self,
        tick: clock::Sample,
    ) -> Result<Ready<'_, 'd, modes::App<'_, 'app, 'd, D, S>>, fault::Dispatch> {
        Ready::new(self.begin(tick))
    }

    fn begin(&mut self, tick: clock::Sample) -> Core<'_, 'd, modes::App<'_, 'app, 'd, D, S>> {
        let turn = self.schedule.begin_at(tick);
        Core {
            driver: self.driver.reborrow(),
            turn,
            pending: &mut *self.pending,
            mode: modes::App {
                view: self.view,
                token: &mut *self.token,
            },
        }
    }
}

impl<'app, 'd: 'app, D, S> Roots<'d> for Scope<'_, '_, 'app, 'd, D, S>
where
    D: executor::raw::App<'d>,
{
    type Mode<'turn>
        = modes::App<'turn, 'app, 'd, D, S>
    where
        Self: 'turn;

    fn driver_ref(&self) -> driver::Reference<'d> {
        self.driver.driver_ref()
    }

    fn next_root<'turn>(
        &'turn mut self,
        root: &ready::Slot<'d, routes::Wake>,
        tick: clock::Sample,
    ) -> Result<RootReady<'turn, 'd, Self::Mode<'turn>>, fault::Dispatch> {
        RootReady::new_app(self.begin(tick), root)
    }
}

impl<'run, 'driver, 'd> Boot<'run, 'driver, 'd> {
    pub(crate) fn try_new(
        driver: &'run mut driver::Context<'driver, 'd>,
        schedule: scope::Scope<'driver, 'd>,
        pending: &'run mut Option<Event<'d>>,
        admitted: Result<(), life::Busy>,
    ) -> Result<Self, drive::Error> {
        if pending.is_some() {
            return Err(life::Busy.into());
        }
        admitted?;
        Ok(Self {
            mode: modes::Boot,
            driver,
            schedule,
            pending,
        })
    }

    fn begin(&mut self, tick: clock::Sample) -> Core<'_, 'd, modes::Boot> {
        Core {
            driver: self.driver.reborrow(),
            turn: self.schedule.begin_at(tick),
            pending: &mut *self.pending,
            mode: self.mode,
        }
    }
}

impl<'d> Roots<'d> for Boot<'_, '_, 'd> {
    type Mode<'turn>
        = modes::Boot
    where
        Self: 'turn;

    fn driver_ref(&self) -> driver::Reference<'d> {
        self.driver.driver_ref()
    }

    fn next_root<'turn>(
        &'turn mut self,
        root: &ready::Slot<'d, routes::Wake>,
        tick: clock::Sample,
    ) -> Result<RootReady<'turn, 'd, Self::Mode<'turn>>, fault::Dispatch> {
        RootReady::new_boot(self.begin(tick), root)
    }
}

impl<'app, 'd: 'app, D, S> Core<'_, 'd, modes::App<'_, 'app, 'd, D, S>>
where
    D: executor::raw::App<'d>,
{
    pub(super) fn shutdown(&self) -> schedule::Progress<'d> {
        match self
            .mode
            .view
            .stopping(
                self.mode.token,
                self.driver.regions(),
                self.driver.driver_ref().clock(),
            )
            .reduce(self.maintenance())
        {
            state @ schedule::Progress::Waiting(_) => state.reduce(self.timer()),
            state => state,
        }
    }
}

impl<'d, M> Core<'_, 'd, M>
where
    M: modes::Mode<'d>,
{
    pub(super) fn timer(&self) -> schedule::Progress<'d> {
        match self.driver.driver_ref().timer().earliest() {
            Some(due) => schedule::Progress::until(due),
            None => schedule::Progress::Quiescent,
        }
    }

    pub(super) fn maintenance(&self) -> schedule::Progress<'d> {
        self.driver.driver_ref().maintenance()
    }

    pub(super) fn timeout(
        &self,
        state: schedule::Progress<'d>,
        source: impl Time,
    ) -> Option<time::Duration> {
        if matches!(state, schedule::Progress::Runnable)
            || self.turn.is_exhausted()
            || self.driver.driver_ref().ready().has_ready()
        {
            return Some(time::Duration::ZERO);
        }
        match state {
            schedule::Progress::Waiting(wait) => wait
                .deadline()
                .map(|due| due.into_raw().saturating_duration_since(source.park())),
            _ => None,
        }
    }

    pub(super) fn is_quiet(&self) -> bool {
        !self.turn.is_exhausted() && !self.driver.driver_ref().ready().has_ready()
    }
}
