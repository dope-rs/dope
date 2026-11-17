use std::{ops::ControlFlow, pin::Pin};

use dope_core::driver::{
    route,
    route::{KeyTag, kind},
    schedule, settings,
};
use dope_runtime::executor;

use crate::harness::observe::Trace;

impl<'d, const N: usize> executor::raw::Layout<'d> for &Trace<route::Erased<'d, kind::Any>, N> {
    type ParkPlan = ();
}

// SAFETY: the borrowed trace has no pinned owners or operation authorities.
unsafe impl<'d, const N: usize> executor::raw::App<'d> for &Trace<route::Erased<'d, kind::Any>, N> {
    const ROUTES: settings::Routes = settings::Routes::singleton::<KeyTag<0>>();
    const WAKE_ROUTES: settings::Routes = Self::ROUTES;
    const COORDS: usize = 0;
    const PARK: bool = false;

    fn select_park() -> Self::ParkPlan {}

    /// # Safety
    /// The root and driver authority belong to the same installation.
    unsafe fn park<'app, 'driver, 'turn>(
        _: Pin<&'app mut Self>,
        _: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'd>,
    ) where
        'd: 'driver + 'turn,
    {
    }

    fn dispatch<'event>(
        _: Pin<&mut Self>,
        _: executor::raw::Dispatch<'event, 'd>,
        _: schedule::Turn<'_, 'd>,
        _: dope_core::driver::Context<'_, 'd>,
    ) -> ControlFlow<executor::raw::Dispatch<'event, 'd>> {
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
