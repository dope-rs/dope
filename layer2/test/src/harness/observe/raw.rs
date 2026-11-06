use std::{ops::ControlFlow, pin::Pin};

use dope_core::{
    driver::{
        route,
        route::{KeyTag, kind},
        schedule, settings,
    },
    io,
};
use dope_runtime::executor;

use crate::harness::observe::Trace;

impl<'d, const N: usize> executor::raw::Layout<'d> for &Trace<route::Erased<'d, kind::Any>, N> {
    type ParkPlan = std::convert::Infallible;
}

// SAFETY: the borrowed trace has no pinned owners or operation authorities.
unsafe impl<'d, const N: usize> executor::raw::App<'d> for &Trace<route::Erased<'d, kind::Any>, N> {
    const ROUTES: settings::Routes = settings::Routes::singleton::<KeyTag<0>>();
    const WAKE_ROUTES: settings::Routes = Self::ROUTES;
    const COORDS: usize = 0;

    fn select_park() -> Option<Self::ParkPlan> {
        None
    }

    /// # Safety
    /// The selected plan is uninhabited, so this operation cannot be invoked.
    unsafe fn park<'app, 'driver, 'turn>(
        _: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'd>,
    ) where
        'd: 'driver + 'turn,
    {
        match *plan {}
    }

    fn dispatch<'app, 'driver, 'turn>(
        _: executor::raw::Dispatch<'app, 'driver, 'turn, 'd, Self>,
    ) -> ControlFlow<io::Event<'d>> {
        ControlFlow::Continue(())
    }

    fn progress(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    /// # Safety
    /// The runtime supplies this exact pinned reference and route-zero event.
    unsafe fn activate<'app, 'driver, 'turn>(
        app: Pin<&'app mut Self>,
        event: executor::raw::Activate<'driver, 'turn, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn,
    {
        app.get_mut().record(event.target());
        true
    }
}
