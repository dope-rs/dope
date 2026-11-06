use std::pin;

use dope_core::driver::{route::Identity, schedule};
use dope_net::Transport;

use crate::{
    app::{dispatch, env},
    client::{session, session::policy, side::mode::Mode},
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
    Self: dispatch::raw::Install<'d>,
    super::Engine<'q, 'h, 'p, 'w, 'd, ID, N, R, I, E, ENDPOINTS, discover::Silent, X>:
        dispatch::raw::Manifold<
                'd,
                Route = ID,
                Probe = <Self as dispatch::raw::Projection<'d>>::Inner,
                PrePark = dispatch::raw::Retained,
                Shutdown = dispatch::raw::Retained,
            > + dispatch::raw::Probe<'d, Plan = ()>,
{
    type Route = ID;
    type Dispatch = dispatch::raw::Projected<Self>;
    type Install = dispatch::raw::Plain;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self> {
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

    fn shutdown<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self> {
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
