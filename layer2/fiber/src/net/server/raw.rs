use std::pin;

use dope::{
    core::driver::route::Identity,
    net,
    net::{
        link::pool::recv::{delivery, input, reserve::Choice},
        wire::{open, receive::Policy},
    },
};
use o3::mem::quota;

use crate::{
    core::driver::{route, schedule},
    manifold::{
        app::{dispatch, dispatch::event},
        network::listener,
    },
    net::server::{Inner, Listener, requests::apply},
};

// SAFETY: Listener forwards the complete lifecycle to the pinned inner owner.
unsafe impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> dispatch::raw::Manifold<'d> for Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Route = ID;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Omit;
    type Finish = Self;
    type Activate = dispatch::raw::Retained;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize =
        <Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let port = self.as_ref().get_ref().port;
        let work = turn.reborrow().app();
        port.connections.pre_park(work, driver.region_token());
        loop {
            let admit = turn
                .reborrow()
                .maint()
                .claim(|| port.connections.pop_deferred());
            let (req, permit) = match admit {
                quota::Admission::Item(item) => item,
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            };
            let conn = req.token;
            permit.run(|| {
                let resume = listener::raw::Resume::from_id(self.as_mut().project().inner, conn);
                apply(resume, Some(req.requests), turn.reborrow(), driver);
            });
        }
        // SAFETY: inherited from this installed owner drive call.
        unsafe { dispatch::raw::Manifold::pre_park(self.as_mut().project().inner, turn, driver) };
        port.retain_error(self.as_ref().get_ref().inner.accept_error());
        if port.accept_error.get().is_some() {
            let _ = port.waiters.wake(work);
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.port.connections.shutdown();
        dispatch::raw::Manifold::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

// SAFETY: selection and projection preserve the exact pinned listener and plan.
unsafe impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> dispatch::raw::Probe<'d, Self> for Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        <Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> as dispatch::raw::Probe<'d>>::select()
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, Self::Plan>) -> schedule::Progress<'d> {
        let own = call.owner().port.progress();
        own.reduce(
            <Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> as dispatch::raw::Probe<'d>>::run(
                call.map_owner(|owner| owner.project_ref().inner),
            ),
        )
    }

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, Self::Plan>) -> schedule::Progress<'d> {
        let own = call.owner().port.progress();
        own.reduce(
            <Inner<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F> as dispatch::raw::Probe<'d>>::stop(
                call.map_owner(|owner| owner.project_ref().inner),
            ),
        )
    }
}

// SAFETY: Listener validates every wake through its owner-bound Resume.
unsafe impl<
    'n,
    'p,
    's,
    'scope,
    'w,
    'd: 'w,
    ID: Identity,
    T: net::Transport,
    W: input::Execution + 'd,
    F: FnMut(open::Error<'w, W>),
> dispatch::raw::Activate<'d, ID> for Listener<'n, 'p, 's, 'scope, 'w, 'd, ID, T, W, F>
where
    <W::Receive as Policy<W>>::Reserve: Choice<W, W::Retained>,
    <W::Receive as Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Access = dispatch::raw::Retained;

    unsafe fn activate<'turn>(
        mut self: pin::Pin<&mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        let port = self.as_ref().get_ref().port;
        let Some(resume) =
            listener::raw::Resume::from_target(self.as_mut().project().inner, target)
        else {
            return;
        };
        let conn = resume.id();
        let reqs = port.connections.requests(conn);
        apply(resume, reqs, turn, driver);
    }
}
