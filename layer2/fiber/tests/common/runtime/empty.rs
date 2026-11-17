use std::{ops::ControlFlow, pin::Pin};

use dope::{
    core::driver::{schedule, schedule::Progress, settings, settings::Routes},
    runtime::executor,
};
use dope_core::driver::route::KeyTag;

pub struct Empty;

impl<'d> executor::raw::Layout<'d> for Empty {
    type ParkPlan = ();
}

// SAFETY: this fixture has no fields or pinned owners and consumes no event authority.
unsafe impl<'d> executor::raw::App<'d> for Empty {
    const ROUTES: settings::Routes = Routes::singleton::<KeyTag<0>>();
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
        Progress::Quiescent
    }
    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        Progress::Quiescent
    }
}
