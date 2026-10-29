use std::pin;

use dope_core::driver::{route, route::Identity, schedule};
use o3::buffer;

use crate::{
    app::{dispatch, dispatch::event},
    network::{
        datagram,
        datagram::{hook, hook::ingress, send},
    },
};

// SAFETY: the endpoint owns all retained socket phases through finish.
unsafe impl<'d, ID: Identity, H, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    dispatch::raw::Manifold<'d> for datagram::Endpoint<'d, ID, H, B, M>
where
    H: datagram::Ingress<'d, ID>,
    H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    <H::Hooks as hook::Bundle>::Install:
        dispatch::raw::Installs<'d, datagram::Endpoint<'d, ID, H, B, M>>,
    <H::Hooks as hook::Bundle>::Shutdown:
        hook::Shutdown<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>,
    <H::Hooks as hook::Bundle>::Finalize:
        hook::Finalize<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>,
    <H::Hooks as hook::Bundle>::Drive: hook::raw::Drive<'d, ID, H, <H::Hooks as hook::Bundle>::Settlement>
        + hook::Schedule<'d, ID, H>,
    H::Mode: ingress::Mode<<H::Hooks as hook::Bundle>::Settlement>,
    <H::Mode as ingress::Mode<<H::Hooks as hook::Bundle>::Settlement>>::Dispatch:
        event::Set<'d, datagram::Endpoint<'d, ID, H, B, M>, ID>,
{
    type Route = ID;
    type Dispatch = <H::Mode as ingress::Mode<<H::Hooks as hook::Bundle>::Settlement>>::Dispatch;
    type Install = <H::Hooks as hook::Bundle>::Install;
    type Finish = Self;
    type Activate = dispatch::raw::Plain;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = <<H::Hooks as hook::Bundle>::Drive as hook::raw::Drive<
        'd,
        ID,
        H,
        <H::Hooks as hook::Bundle>::Settlement,
    >>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        // SAFETY: the caller retains this exact installed endpoint through quiescence.
        unsafe {
            <<H::Hooks as hook::Bundle>::Drive as hook::raw::Drive<
                'd,
                ID,
                H,
                <H::Hooks as hook::Bundle>::Settlement,
            >>::pre_park(self, turn, driver)
        }
    }

    fn shutdown<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        <<H::Hooks as hook::Bundle>::Shutdown as hook::Shutdown<
            'd,
            ID,
            H,
            <H::Hooks as hook::Bundle>::Settlement,
        >>::shutdown(self, turn, driver)
    }
}

// SAFETY: unit selection binds every query to this exact pinned endpoint.
unsafe impl<'d, ID: Identity, H, B: buffer::Residency + buffer::raw::Source, M: send::Mode>
    dispatch::raw::Probe<'d, Self> for datagram::Endpoint<'d, ID, H, B, M>
where
    H: datagram::Ingress<'d, ID>,
    H::Hooks: hook::Bundle<Buffer = B, Send = M>,
    <H::Hooks as hook::Bundle>::Drive: hook::Schedule<'d, ID, H>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        let socket = call
            .owner()
            .as_ref()
            .project_ref()
            .socket
            .progress(call.region());
        let Some(plan) =
            <<H::Hooks as hook::Bundle>::Drive as hook::Schedule<'d, ID, H>>::select(call.clock())
        else {
            return socket;
        };
        socket.reduce(<<H::Hooks as hook::Bundle>::Drive as hook::Schedule<
            'd,
            ID,
            H,
        >>::progress(call.owner(), plan, call.state()))
    }
}

// SAFETY: activation resumes only the endpoint's pinned receive phase.
unsafe impl<'d, ID: Identity, H, B, M: send::Mode> dispatch::raw::Activate<'d, ID>
    for datagram::Endpoint<'d, ID, H, B, M>
{
    type Access = dispatch::raw::Plain;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _: route::Target<'d, ID>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        self.project().socket.project().receive.resume();
    }
}

// SAFETY: the installed control borrows this exact pinned endpoint for the view lifetime.
unsafe impl<'d, ID: Identity, H, B, M: send::Mode> dispatch::raw::Control<'d>
    for datagram::Endpoint<'d, ID, H, B, M>
where
    Self: dispatch::raw::Manifold<'d>,
{
    type View<'step>
        = datagram::View<'step, 'd, ID, H, B, M>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        datagram::View::new(owner)
    }
}
