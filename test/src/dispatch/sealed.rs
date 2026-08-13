use std::pin;

use dope::{
    core::{
        driver::{retained, route, schedule::ready},
        io,
    },
    runtime::executor,
};
use o3::cell::region;

pub(in crate::dispatch) trait ReadyState<'d>: Unpin {}

impl<'d> ReadyState<'d> for () {}
impl<'d, Tag: route::Tag> ReadyState<'d> for ready::Slot<'d, Tag> {}
impl<'d, Tag: route::Tag> ReadyState<'d> for Box<[ready::Slot<'d, Tag>]> {}
impl<'d> ReadyState<'d> for &mut Vec<route::Token> {}

pub(in crate::dispatch) trait Proof<'d>: Sized {
    fn dispatch<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Dispatch<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (pin::Pin<&'pin mut Self>, io::Event<'d>)
    where
        'd: 'app;

    fn activate<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Activate<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        route::Token,
        retained::Context<'driver, 'app, 'd>,
    )
    where
        'd: 'app;

    fn pre_park<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::PrePark<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        retained::Context<'driver, 'app, 'd>,
    )
    where
        'd: 'app;

    fn progress<'pin, 'region>(
        call: executor::raw::Progress<'pin, 'region, 'd, Self>,
    ) -> (pin::Pin<&'pin Self>, &'region region::Token<'d>);

    fn shutdown<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Shutdown<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        executor::raw::ShutdownRoot<'driver, 'app, 'd, Self>,
    )
    where
        'd: 'app;

    fn finish<'pin, 'finalization, 'app>(
        call: executor::raw::Finish<'pin, 'finalization, 'app, 'd, Self>,
    ) -> pin::Pin<&'pin mut Self>
    where
        'd: 'app;
}

impl<'d, R, H, const ID: u8, const FILTER: bool> Proof<'d> for super::Probe<'d, R, H, ID, FILTER> {
    fn dispatch<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Dispatch<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (pin::Pin<&'pin mut Self>, io::Event<'d>)
    where
        'd: 'app,
    {
        // SAFETY: Probe never installs or retains an application-owned backend resource.
        let (self_, event, _, _) = unsafe { call.into_parts_unchecked() };
        (self_, event)
    }

    fn activate<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Activate<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        route::Token,
        retained::Context<'driver, 'app, 'd>,
    )
    where
        'd: 'app,
    {
        // SAFETY: Probe never installs or retains an application-owned backend resource.
        let (self_, target, _, driver) = unsafe { call.into_parts_unchecked() };
        (self_, target, driver)
    }

    fn pre_park<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::PrePark<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        retained::Context<'driver, 'app, 'd>,
    )
    where
        'd: 'app,
    {
        // SAFETY: Probe never installs or retains an application-owned backend resource.
        let (self_, _, driver) = unsafe { call.into_parts_unchecked() };
        (self_, driver)
    }

    fn progress<'pin, 'region>(
        call: executor::raw::Progress<'pin, 'region, 'd, Self>,
    ) -> (pin::Pin<&'pin Self>, &'region region::Token<'d>) {
        // SAFETY: Probe never installs or retains an application-owned backend resource.
        unsafe { call.into_parts_unchecked() }
    }

    fn shutdown<'pin, 'driver, 'turn, 'app>(
        call: executor::raw::Shutdown<'pin, 'driver, 'turn, 'app, 'd, Self>,
    ) -> (
        pin::Pin<&'pin mut Self>,
        executor::raw::ShutdownRoot<'driver, 'app, 'd, Self>,
    )
    where
        'd: 'app,
    {
        // SAFETY: Probe has no retained owners, so the exact shutdown proof is complete.
        let (self_, _, shutdown) = unsafe { call.into_parts_unchecked() };
        (self_, shutdown)
    }

    fn finish<'pin, 'finalization, 'app>(
        call: executor::raw::Finish<'pin, 'finalization, 'app, 'd, Self>,
    ) -> pin::Pin<&'pin mut Self>
    where
        'd: 'app,
    {
        // SAFETY: Probe has no retained owners requiring post-quiescence finalization.
        let (self_, _) = unsafe { call.into_parts_unchecked() };
        self_
    }
}
