use std::{mem, pin};

use dope_core::driver::{
    retained,
    schedule::{self, clock},
};
use o3::mem::quota;

use crate::{control, executor};

/// Pre-park authority for one exact retained root and scheduler turn.
#[must_use]
pub struct Park<'driver, 'turn, 'root, 'd: 'root> {
    drive: executor::raw::Drive<'driver, 'turn, 'root, 'd>,
}

impl<'driver, 'turn, 'root, 'd: 'root> Park<'driver, 'turn, 'root, 'd> {
    pub(crate) const fn new(
        turn: schedule::Turn<'turn, 'd>,
        driver: retained::Context<'driver, 'root, 'd>,
    ) -> Self {
        Self {
            drive: executor::raw::Drive::new(turn, driver),
        }
    }

    pub fn reborrow(&mut self) -> Park<'_, '_, 'root, 'd> {
        Park {
            drive: self.drive.reborrow(),
        }
    }

    pub fn map<R>(
        self,
        map: impl FnOnce(schedule::Turn<'turn, 'd>, retained::Context<'driver, 'root, 'd>) -> R,
    ) -> R {
        let (turn, driver) = self.drive.into_parts();
        map(turn, driver)
    }

    pub fn into_driver(self) -> retained::Context<'driver, 'root, 'd> {
        self.drive.into_driver()
    }

    pub(crate) fn run<R>(self, app: pin::Pin<&mut R>, plan: &mut Option<R::ParkPlan>)
    where
        R: executor::raw::App<'d>,
    {
        if let Some(plan) = plan {
            // SAFETY: publication prepared this plan for the same installed pinned root.
            // Failed admission runs forced finish without entering a park operation.
            unsafe { R::park(app, plan, self) }
        }
    }

    pub fn app_cursor<const N: usize>(
        &self,
        cursor: &mut executor::raw::Cursor<N>,
    ) -> executor::raw::Cursor<N> {
        cursor.start(self.drive.turn.app().remaining())
    }

    pub fn maint_cursor<const N: usize>(
        &self,
        cursor: &mut executor::raw::Cursor<N>,
    ) -> executor::raw::Cursor<N> {
        cursor.start(self.drive.turn.maint().remaining())
    }

    pub fn coord_cursor<const N: usize>(
        &self,
        cursor: &mut executor::raw::Cursor<N>,
    ) -> executor::raw::Cursor<N> {
        cursor.start(self.drive.turn.coord().remaining())
    }

    pub fn coord_left(&self) -> usize {
        self.drive.turn.coord().remaining()
    }

    pub fn share<const N: usize, R>(
        &mut self,
        ranks: [usize; 3],
        weights: [usize; 2],
        run: impl FnOnce(Park<'_, '_, 'root, 'd>) -> R,
    ) -> R {
        self.drive.turn.share::<N, _>(ranks, weights, |turn| {
            let drive = executor::raw::Drive::new(turn, self.drive.driver());
            run(Park { drive })
        })
    }

    pub fn share_coord<const N: usize, R>(
        &mut self,
        rank: usize,
        weights: [usize; 2],
        run: impl FnOnce(Park<'_, '_, 'root, 'd>) -> R,
    ) -> R {
        self.drive
            .turn
            .coord()
            .share::<N, _>(rank, weights, || run(self.reborrow()))
    }

    /// Monotonic time captured once at the beginning of this driver turn.
    pub fn now(&mut self) -> clock::Moment<'d> {
        self.drive.driver().driver_ref().clock().now()
    }

    /// Admits one step whose clock and region authorities share this borrow.
    pub fn next_step(&mut self) -> Option<control::Step<'_, 'd>> {
        match self.drive.turn().coord().take() {
            quota::Take::Taken => {}
            quota::Take::Exhausted => return None,
        }
        let driver = self.drive.driver().into_driver();
        Some(control::Step::new(driver))
    }
}

const fn assert_layout() {
    type Turn<'a> = Park<'a, 'a, 'a, 'a>;

    assert!(mem::size_of::<Turn<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Turn<'static>>() == mem::align_of::<usize>());
}

const _: () = assert_layout();
