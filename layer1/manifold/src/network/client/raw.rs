use std::pin;

use dope_core::driver::{route::Identity, schedule};
use dope_net::Transport;
use session::policy;

use crate::{
    app::{dispatch, env},
    client::{conn::engine, session, side},
    network::client::{Connector, cycle::Cycle},
    service::{discover, sync},
};

// SAFETY: every pinned lifecycle step is delegated to the engine that owns this
// connector's installation and quiescence.
unsafe impl<
    'q,
    'h,
    'p,
    'w,
    'd: 'w,
    ID: Identity,
    const MAX_CONNECTIONS: usize,
    N,
    R,
    I,
    E,
    const ENDPOINTS: usize,
    D,
    O,
> dispatch::raw::Manifold<'d>
    for Connector<'q, 'h, 'p, 'w, 'd, ID, MAX_CONNECTIONS, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX_CONNECTIONS> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
    Self: Cycle<'d> + dispatch::raw::Probe<'d, Self, Plan = ()>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled>:
        dispatch::raw::Manifold<
                'd,
                Route = ID,
                PrePark = dispatch::raw::Retained,
                Shutdown = dispatch::raw::Retained,
            >,
{
    type Route = ID;
    type Dispatch = dispatch::raw::Projected<Self>;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut owner = Cycle::prepare(self, turn.reborrow(), driver.state_token());
        // SAFETY: `Cycle::prepare` keeps the connector pinned, so the projected
        // engine stays installed for this dispatch.
        unsafe { dispatch::raw::Manifold::pre_park(owner.as_mut().project().engine, turn, driver) };
        owner
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

// SAFETY: shutdown and stopped progress delegate to the exact engine; discovery
// owns no retained native submissions and cannot hide the engine's pending I/O.
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
    D,
    O,
> dispatch::raw::Resume<'d> for Connector<'q, 'h, 'p, 'w, 'd, ID, MAX, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
    Self: dispatch::raw::Manifold<'d>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled>:
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
    D,
    O,
> dispatch::raw::Probe<'d, Self>
    for Connector<'q, 'h, 'p, 'w, 'd, ID, MAX, N, D, R, I, O, E, ENDPOINTS>
where
    N: session::Target<'d, ID, MAX> + policy::Compatible<'w, 'd, E::Wire, ID>,
    E: env::Spec,
    E::Wire: 'd,
    E::Transport: Transport,
    I: Eq,
    R: sync::Policy,
    D: discover::Driver<'d>,
    Self: Cycle<'d>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled>:
        dispatch::raw::Probe<'d, Plan = ()>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        <super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, D::Feedback, side::Disabled> as dispatch::raw::Probe<'d>>::select()
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, Self::Plan>) -> schedule::Progress<'d> {
        let owner = call.owner();
        let this = owner.project_ref();
        if this.engine.control().is_retiring() {
            return schedule::Progress::Runnable;
        }
        let wait = Cycle::wait(owner, call.state());
        <engine::Machine<'_, '_, '_, ID, _, _, E, _> as dispatch::raw::Probe<'d>>::run(
            call.map_owner(|owner| owner.project_ref().engine),
        )
        .reduce(wait)
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
