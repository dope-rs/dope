use std::{cell::Cell, pin::Pin};

use dope_core::{
    Errno,
    driver::{schedule, settings},
};
use dope_runtime::{client, executor, topology::lending};

/// Lifecycle digits: install 1, stop 2, finish 3, drop 4, client drop 5, park 6.
pub struct Observer<'a, const COORDS: usize> {
    pub events: &'a Cell<u64>,
    pub stalled: bool,
    pub cause: Errno,
}

impl<'d, const COORDS: usize> executor::raw::Layout<'d> for Observer<'_, COORDS> {
    type ParkPlan = ();
}

// SAFETY: this fixture owns no routes or native operations. Every lifecycle
// visit preserves its exact pinned owner and the authority supplied by Runtime.
unsafe impl<'d, const COORDS: usize> executor::raw::App<'d> for Observer<'_, COORDS> {
    const ROUTES: settings::Routes = settings::Routes::EMPTY;
    const COORDS: usize = COORDS;

    fn install<'app, 'owner>(
        call: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        let (app, install) = call.into_parts();
        record(app.events, 1);
        executor::raw::Install::new(app, install)
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(
        app: Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        if app.stalled {
            schedule::Progress::waiting(&view.regions().0)
        } else {
            schedule::Progress::Quiescent
        }
    }

    fn select_park() -> Option<Self::ParkPlan> {
        Some(())
    }

    /// # Safety
    /// The runtime retains this exact installed owner and its admitted park plan.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        _: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        if app.events.get() & 0xf != 6 {
            record(app.events, 6);
        }
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        call: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (app, turn, driver) = call.into_parts();
        record(app.events, 2);
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (app, install, finish) = call.into_parts();
        record(app.events, 3);
        finish.fail_disarm(app.cause);
        executor::raw::Finish::new(app, install, finish)
    }
}

impl<const COORDS: usize> Drop for Observer<'_, COORDS> {
    fn drop(&mut self) {
        record(self.events, 4);
    }
}

// SAFETY: after quiescence the observer retains only its borrowed Cell and
// exact error value; neither it nor its client submits native cleanup work.
unsafe impl<'d> lending::raw::Resume<'d> for Observer<'_, 0> {}

pub struct Client<'app, 'd: 'app, 'a>(client::Bound<'app, 'd, Observer<'a, 0>, &'a Cell<u64>>);

impl<'d, 'a> client::Provider<'d> for Observer<'a, 0> {
    type Client<'app>
        = Client<'app, 'd, 'a>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (owner, bound) = context.into_parts();
        Client(bound.bind(owner.events))
    }
}

impl Drop for Client<'_, '_, '_> {
    fn drop(&mut self) {
        record(&self.0, 5);
    }
}

#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
pub struct Child<'state, 'app, 'd: 'app, 'a> {
    #[dispatcher(state)]
    pub client: Pin<&'state Client<'app, 'd, 'a>>,
}

pub struct Output<'a>(pub &'a Cell<bool>);

impl Drop for Output<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

fn record(events: &Cell<u64>, digit: u64) {
    events.set((events.get() << 4) | digit);
}
