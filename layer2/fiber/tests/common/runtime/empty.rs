use std::{ops::ControlFlow, pin::Pin};

use dope::{
    core::{
        driver::{schedule, schedule::Progress, settings, settings::Routes},
        io,
    },
    runtime::executor,
};
use dope_core::driver::route::KeyTag;

pub struct Empty;

impl<'d> executor::raw::Layout<'d> for Empty {
    type ParkPlan = std::convert::Infallible;
}

// SAFETY: this fixture has no fields or pinned owners and consumes no event authority.
unsafe impl<'d> executor::raw::App<'d> for Empty {
    const ROUTES: settings::Routes = Routes::singleton::<KeyTag<0>>();
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
        Progress::Quiescent
    }
    fn stopping(_: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        Progress::Quiescent
    }
}
