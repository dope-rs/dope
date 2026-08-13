use std::ops;

use dope_core::{driver::schedule, io};

use crate::executor;

/// Application branded by one generative driver lifetime.
/// Lifecycle calls carry its exact pinned application; the derive owns any
/// structural decomposition.
pub trait Application<'d>: Sized {
    #[doc(hidden)]
    fn install<'app>(call: executor::raw::Install<'_, 'app, 'd, Self>)
    where
        'd: 'app,
    {
        let _ = call;
    }

    #[doc(hidden)]
    fn dispatch<'driver, 'turn, 'app>(
        call: executor::raw::Dispatch<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'app;

    #[doc(hidden)]
    fn activate<'driver, 'turn, 'app>(
        call: executor::raw::Activate<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app;

    #[doc(hidden)]
    fn pre_park<'driver, 'turn, 'app>(
        call: executor::raw::PrePark<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app;

    #[doc(hidden)]
    fn progress(call: executor::raw::Progress<'_, '_, 'd, Self>) -> schedule::Progress<'d>;

    #[doc(hidden)]
    fn shutdown_progress(
        call: executor::raw::Progress<'_, '_, 'd, Self>,
    ) -> schedule::Progress<'d> {
        Self::progress(call)
    }

    #[doc(hidden)]
    fn shutdown<'driver, 'turn, 'app>(
        call: executor::raw::Shutdown<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> executor::raw::Pending<'app, 'd, Self>
    where
        'd: 'app;

    #[doc(hidden)]
    fn finish<'finalization, 'app>(call: executor::raw::Finish<'_, 'finalization, 'app, 'd, Self>)
    where
        'd: 'app,
    {
        let _ = call;
    }
}

impl<'d> Application<'d> for () {
    fn dispatch<'driver, 'turn, 'app>(
        call: executor::raw::Dispatch<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'app,
    {
        call.consume()
    }

    fn activate<'driver, 'turn, 'app>(
        call: executor::raw::Activate<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app,
    {
        let _ = call;
    }

    fn pre_park<'driver, 'turn, 'app>(
        call: executor::raw::PrePark<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) where
        'd: 'app,
    {
        let _ = call;
    }

    fn progress(_call: executor::raw::Progress<'_, '_, 'd, Self>) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn shutdown<'driver, 'turn, 'app>(
        call: executor::raw::Shutdown<'_, 'driver, 'turn, 'app, 'd, Self>,
    ) -> executor::raw::Pending<'app, 'd, Self>
    where
        'd: 'app,
    {
        call.complete()
    }
}
