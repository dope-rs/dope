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
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        _: Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        match *plan {}
    }

    fn dispatch<'app, 'driver, 'turn, 'root>(
        _: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
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
    unsafe fn activate<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        event: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        app.get_mut().record(event.target());
        true
    }
}
