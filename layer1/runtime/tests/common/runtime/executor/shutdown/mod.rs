//! Runtime shutdown integration coverage.

use std::{cell::Cell, pin, rc::Rc, thread, time};

use dope_core::driver::{retained, schedule, schedule::clock, settings};
use dope_runtime::{executor, shutdown};
use o3::base::cell::region;

use crate::common::scenario::dispatch;

pub mod budget;
pub mod roots;
pub mod timeouts;

type Flag = Rc<Cell<bool>>;

pub struct Count {
    pub stops: Rc<Cell<Option<usize>>>,
    pub ends: Rc<Cell<Option<usize>>>,
    pub root: Option<(Flag, Flag)>,
}

impl<'d> dispatch::Hooks<'d, ()> for Count {
    type Park = dispatch::Off;

    fn progress(
        &self,
        _: &(),
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        if let Some((root, ordered)) = &self.root {
            ordered.set(ordered.get() && root.get());
        }
        schedule::Progress::Quiescent
    }

    fn shutdown(&mut self, _: &mut ()) {
        if let Some((root, ordered)) = &self.root {
            ordered.set(ordered.get() && root.get());
        }
        self.stops
            .set(self.stops.get().and_then(|stops| stops.checked_add(1)));
    }

    fn finish(&mut self, _: &mut ()) {
        self.ends
            .set(self.ends.get().and_then(|ends| ends.checked_add(1)));
    }
}

pub struct Storm {
    pub calls: Rc<Cell<Option<usize>>>,
}

impl<'d> executor::raw::Layout<'d> for Storm {
    type ParkPlan = ();
}

unsafe impl<'d> executor::raw::App<'d> for Storm {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 1;

    fn progress(
        _: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(
        _: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    /// # Safety
    /// The pinned Storm and prepared unit plan must belong to this installed
    /// root; the supplied park authority bounds that root's coordinator work.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        mut app: pin::Pin<&'app mut Storm>,
        (): &mut Self::ParkPlan,
        mut park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        while park.next_step().is_some() {
            let this = app.as_mut().get_mut();
            this.calls
                .set(this.calls.get().and_then(|calls| calls.checked_add(1)));
        }
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        shutdown: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Storm>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Storm>
    where
        'd: 'owner,
    {
        let (mut app, turn, driver) = shutdown.into_parts();
        let calls = &app.as_mut().get_mut().calls;
        calls.set(calls.get().map(|_| 0));
        executor::raw::Shutdown::new(app, turn, driver)
    }
}

#[pin_project::pin_project]
pub struct DrainProbe<F> {
    pub begun: Flag,
    pub forced: Flag,
    pub remaining: usize,
    pub shutdown: Option<(shutdown::Trigger, time::Duration)>,
    pub fired: F,
}

impl<'d, F> executor::raw::Layout<'d> for DrainProbe<F> {
    type ParkPlan = ();
}

unsafe impl<'d, F: Fn(Result<(), shutdown::FireError>)> executor::raw::App<'d> for DrainProbe<F> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = 1;

    fn progress(
        app: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        if app.remaining == 0 {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::Runnable
        }
    }

    fn stopping(
        _: pin::Pin<&Self>,
        _: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    /// # Safety
    /// The pinned probe and prepared unit plan must belong to this installed
    /// root; shutdown progress consumes only its supplied park authority.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        mut app: pin::Pin<&'app mut Self>,
        (): &mut Self::ParkPlan,
        mut park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        let this = app.as_mut().project();
        if let Some((trigger, uptime)) = this.shutdown.take() {
            thread::sleep(uptime);
            (this.fired)(trigger.fire());
        }
        if this.begun.get() && park.next_step().is_some() {
            *this.remaining = this.remaining.saturating_sub(1);
        }
    }
}

// SAFETY: the probe has no routed owners; drain changes only its progress flags.
unsafe impl<'d, F: Fn(Result<(), shutdown::FireError>)> executor::raw::Drain<'d> for DrainProbe<F> {
    fn begin(
        self: pin::Pin<&mut Self>,
        _: schedule::Turn<'_, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.project().begun.set(true);
    }

    fn progress(
        self: pin::Pin<&Self>,
        _: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        if self.remaining == 0 {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::Runnable
        }
    }

    fn force(
        self: pin::Pin<&mut Self>,
        _: schedule::Turn<'_, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        let this = self.project();
        this.forced.set(true);
        *this.remaining = 0;
    }
}
