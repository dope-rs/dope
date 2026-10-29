use std::pin;

use dope_core::driver::{
    route::{
        Identity, {self},
    },
    schedule::{self},
};
use dope_net::link::pool::{
    self, pending,
    recv::{input, reserve::Choice},
};
use dope_runtime::executor;
use o3::base::Call as _;

use crate::{
    app::{
        Park,
        dispatch::{self, event},
        env,
        receive::ingress::{self},
    },
    client::{
        app, attempt,
        conn::{
            self, engine,
            engine::{
                drive::{self},
                liveness,
                shutdown::{self},
                transfer::wakes,
            },
        },
        side::mode::Mode,
    },
};

/// Reunites one retained owner plane with the live slot that shares its lane.
///
/// # Safety
/// `slot` must be `key`'s initialized connection in `owner`'s pool, retaining both borrows.
pub(in crate::client::conn::engine) unsafe fn reunite<'a, 'd, ID: Identity, H, C>(
    owner: &'a mut H,
    slot: C,
    key: pool::Key<'d, ID>,
) -> conn::Source<&'a mut H, C, pool::Key<'d, ID>> {
    // SAFETY: the caller proved that this slot belongs to the owner's exact lane.
    unsafe { conn::Source::new(((owner, slot), key)) }
}

/// Projects the live slot back out of one reunification proof.
pub(in crate::client::conn::engine) fn slot_mut<'a, 'd, ID: Identity, H, C>(
    proof: &'a mut conn::Source<&mut H, C, pool::Key<'d, ID>>,
) -> &'a mut C {
    // SAFETY: the proof retains the exclusive slot borrow it was built from, and
    // projecting it preserves that lane's initialization predicate.
    &mut unsafe { proof.as_mut() }.into_inner().0.1
}

// SAFETY: Engine drives every pinned primary connector through route quiescence.
unsafe impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> dispatch::raw::Manifold<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
    Self: shutdown::Drain<'d>
        + drive::Run<'d>
        + event::Turn<'d, Access = dispatch::raw::Retained>
        + dispatch::raw::Activate<'d, ID, Access = dispatch::raw::Retained>
        + dispatch::raw::Probe<'d, Self, Plan = ()>
        + executor::raw::Finishes<'d, Self>,
{
    type Route = ID;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Omit;
    type Finish = Self;
    type Activate = dispatch::raw::Retained;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        shutdown::Drain::drain(self.as_mut(), turn.reborrow(), driver);
        if let Some(call) = A::park() {
            let this = self.as_mut().project();
            call.call((
                this.app,
                &mut Park::new(
                    turn.reborrow().app(),
                    driver.driver().reborrow().into_regions(),
                ),
            ));
        }
        drive::Run::run(self.as_mut(), turn.reborrow(), driver);
        self.as_mut().flush_recv(turn.maint(), driver);
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
        shutdown::Drain::begin(self.as_mut());
        shutdown::Drain::drain(self.as_mut(), turn, driver);
        self
    }
}

// SAFETY: stopped quiescence requires the primary pool and side I/O to be empty.
// Each connection retires its retained flights before leaving those owners.
unsafe impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> dispatch::raw::Resume<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
    Self: dispatch::raw::Manifold<'d>,
{
}

// SAFETY: unit selection binds every query to this exact pinned engine and selected side mode.
unsafe impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> dispatch::raw::Probe<'d, Self>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        let this = call.owner().project_ref();
        if !pending::Pending::of(this.links.pool()).is_empty()
            || this.links.pool().inspect().has_rearm()
            || this.links.pool().inspect().has_credit()
            || X::is_runnable(this.side, this.links.liveness())
        {
            return schedule::Progress::Runnable;
        }
        let io = if !this.links.pool().inspect().is_empty() || X::has_io(this.links.liveness()) {
            schedule::Progress::waiting(call.region())
        } else {
            schedule::Progress::Quiescent
        };
        this.app.progress(call.region(), call.clock()).reduce(io)
    }

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        let this = call.owner().project_ref();
        if !this.shutdown.is_draining() {
            return Self::run(call);
        }
        if !pending::Pending::of(this.links.pool()).is_empty()
            || this.links.pool().inspect().has_rearm()
            || this.links.pool().inspect().has_credit()
            || X::is_runnable(this.side, this.links.liveness())
        {
            return schedule::Progress::Runnable;
        }
        this.app
            .progress(call.region(), call.clock())
            .reduce(schedule::Progress::waiting(call.region()))
    }
}

// SAFETY: activations carry this engine's canonical installed route.
unsafe impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> dispatch::raw::Activate<'d, ID>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
    Self: ingress::Policy<'p, 'w, 'd, ID, Input = A::Input, Wire = A::Wire>
        + wakes::Activation<'d, ID, A::Continuation>,
{
    type Access = dispatch::raw::Retained;

    unsafe fn activate<'turn>(
        mut self: pin::Pin<&mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        <Self as ingress::Policy<'p, 'w, 'd, ID>>::resume(
            self.as_mut(),
            target,
            turn.reborrow(),
            driver,
        );
        <Self as wakes::Activation<'d, ID, A::Continuation>>::activate(self, target, turn, driver);
    }
}
