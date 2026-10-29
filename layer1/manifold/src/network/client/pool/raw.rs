use std::pin;

use dope_core::driver::{route::Identity, schedule};
use dope_net::Transport;

use crate::{
    app::{dispatch, env},
    client::{conn::engine, session, session::policy, side::mode::Mode},
    network::client::Pool,
    service::{discover, sync},
};

// SAFETY: Pool delegates every retained close to its pinned engine.
unsafe impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    X,
> dispatch::raw::Manifold<'d> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
    Self: dispatch::raw::Install<'d> + dispatch::raw::Probe<'d, Self, Plan = ()>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>:
        dispatch::raw::Manifold<
                'd,
                Route = ID,
                PrePark = dispatch::raw::Retained,
                Shutdown = dispatch::raw::Retained,
            >,
{
    type Route = ID;
    type Dispatch = dispatch::raw::Projected<Self>;
    type Install = dispatch::raw::Plain;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.as_mut().maintain(turn.reborrow());
        // SAFETY: the pinned pool keeps its engine installed through this dispatch.
        unsafe {
            dispatch::raw::Manifold::pre_park(
                self.as_mut().project().engine,
                turn.reborrow(),
                driver,
            )
        };
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
        dispatch::raw::Manifold::shutdown(self.as_mut().project().engine, turn, driver);
        self
    }
}

// SAFETY: this wrapper forwards shutdown and stopped progress to its only
// retained native owner, whose resume contract remains unchanged.
unsafe impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    X,
> dispatch::raw::Resume<'d> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
    Self: dispatch::raw::Manifold<'d>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>:
        dispatch::raw::Resume<'d>,
{
}

// SAFETY: projection preserves this exact pinned engine and its complete liveness plan.
unsafe impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    X,
> dispatch::raw::Probe<'d, Self> for Pool<'q, 'h, 'p, 'w, 'd, ID, MAX, N, R, I, E, ENDPOINTS, X>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    X: Mode<'d, ID>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>:
        dispatch::raw::Probe<'d, Plan = ()>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        <super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X> as dispatch::raw::Probe<'d>>::select()
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, Self::Plan>) -> schedule::Progress<'d> {
        if call.owner().project_ref().engine.control().is_retiring() {
            return schedule::Progress::Runnable;
        }
        <engine::Machine<'_, '_, '_, ID, _, _, E, _> as dispatch::raw::Probe<'d>>::run(
            call.map_owner(|owner| owner.project_ref().engine),
        )
    }

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, Self::Plan>) -> schedule::Progress<'d> {
        if call.owner().project_ref().engine.control().is_retiring() {
            return schedule::Progress::Runnable;
        }
        <engine::Machine<'_, '_, '_, ID, _, _, E, _> as dispatch::raw::Probe<'d>>::stop(
            call.map_owner(|owner| owner.project_ref().engine),
        )
    }
}
