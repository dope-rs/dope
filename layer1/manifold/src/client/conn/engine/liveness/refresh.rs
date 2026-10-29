use std::pin;

use dope_core::driver::{retained, route::Identity, schedule::clock};
use dope_net::link::pool;

use crate::{
    app::env,
    client::{
        app, attempt,
        conn::{
            self, closing,
            engine::{self, liveness},
            state,
        },
        deadline,
    },
    timing,
};

pub(in crate::client::conn::engine) fn run<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>(
    mut machine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    key: pool::Key<'d, ID>,
    now: clock::Moment<'d>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    let target = machine.as_ref().backoff();
    let this = machine.as_mut().project();
    let Some(slot) = this.links.inspect(key, target) else {
        return;
    };
    if matches!(
        closing::Callback::new(&slot.state.closing),
        closing::Callback::Sealed
    ) {
        slot.cancel(deadline::Kind::Inbound);
        return;
    }
    let Some(active) = state::ActiveRef::new(&slot) else {
        return;
    };
    let watch = <A::Continuation as liveness::Inspection<'d, ID, A>>::inspect(
        this.app,
        conn::Ref::new(active),
        <E::Timing as timing::Policy>::IDLE_WINDOW,
        driver,
    );
    match watch {
        liveness::Observation::Quiescent => {
            slot.cancel(deadline::Kind::Inbound);
        }
        liveness::Observation::Awaiting { window, seen } => {
            slot.arm_inbound(now, Some(seen), window);
        }
    }
}
