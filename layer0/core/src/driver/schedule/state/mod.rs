use std::cell;

use o3::mem::quota;

use crate::{
    driver,
    driver::{
        schedule,
        schedule::{ready, timers},
    },
};

pub(crate) mod lanes;
pub mod scope;

pub(super) enum ReactorLane {}
pub(super) enum CoordLane {}
pub(super) enum AppLane {}
pub(super) enum MaintLane {}

#[derive(Clone, Copy)]
pub(super) enum Cursor {
    First,
    Second,
}

impl Cursor {
    const fn next(self) -> Self {
        match self {
            Self::First => Self::Second,
            Self::Second => Self::First,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }
}

pub(super) struct Reactor {
    pub(super) ledger: quota::Ledger<ReactorLane>,
    pub(super) cursor: cell::Cell<Cursor>,
}

pub(super) struct Lanes {
    pub(super) ready: quota::Ledger<ready::Lane>,
    pub(super) coord: quota::Ledger<CoordLane>,
    pub(super) app: quota::Ledger<AppLane>,
    pub(super) timers: quota::Ledger<timers::Lane>,
    pub(super) maint: quota::Ledger<MaintLane>,
}

pub(in crate::driver) struct Core<'d> {
    pub(super) driver: driver::Reference<'d>,
    pub(super) reactor: Reactor,
    pub(super) lanes: Lanes,
    pub(super) deferred: cell::Cell<bool>,
}

impl<'d> Core<'d> {
    pub(in crate::driver) fn new(driver: driver::Reference<'d>) -> Self {
        Self {
            driver,
            reactor: Reactor {
                ledger: quota::Ledger::new(schedule::LIMIT),
                cursor: cell::Cell::new(Cursor::First),
            },
            lanes: Lanes {
                ready: quota::Ledger::new(schedule::LIMIT),
                coord: quota::Ledger::new(schedule::LIMIT),
                app: quota::Ledger::new(schedule::LIMIT),
                timers: quota::Ledger::new(schedule::LIMIT),
                maint: quota::Ledger::new(schedule::LIMIT),
            },
            deferred: cell::Cell::new(false),
        }
    }

    pub(super) fn reset(&mut self) {
        self.deferred.set(false);
        self.reactor.ledger.reset(schedule::LIMIT);
        self.lanes.ready.reset(schedule::LIMIT);
        self.lanes.coord.reset(schedule::LIMIT);
        self.lanes.app.reset(schedule::LIMIT);
        self.lanes.timers.reset(schedule::LIMIT);
        self.lanes.maint.reset(schedule::LIMIT);
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.deferred.get()
            || self.reactor.ledger.remaining() == 0
            || self.lanes.ready.remaining() == 0
            || self.lanes.app.remaining() == 0
            || self.lanes.timers.remaining() == 0
            || self.lanes.maint.remaining() == 0
    }
}
