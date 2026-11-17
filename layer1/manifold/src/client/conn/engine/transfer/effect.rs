use dope_core::driver::{self, route::Identity, schedule};

use crate::{
    app::{env, receive},
    client::{
        app, attempt,
        conn::{
            engine::{self, liveness, ops::close, transfer::send},
            flow,
        },
        side::mode::Mode,
    },
};

pub(in crate::client::conn::engine) trait Effect<'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn apply<'entry, 'p, 'turn, 'driver, 'context>(
        self,
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Option<engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>>, receive::Outcome)
    where
        'd: 'p,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
        A::Requests: send::Policy<'w, 'd, ID, A>;
}

impl<'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Effect<'w, 'd, ID, A, S, E, X> for app::ChunkOutcome
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn apply<'entry, 'p, 'turn, 'driver, 'context>(
        self,
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Option<engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>>, receive::Outcome)
    where
        'd: 'p,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        if let Err(error) = self.into_result() {
            return (Some(entry), receive::Outcome::Failed(error));
        }
        let close = entry.get().state.closing.is_closing();
        let entry = match flow {
            flow::Flow::Continue(()) => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit_entry::<false, false, S, E, X>(
                    context, entry, turn, driver,
                )
            }
            flow::Flow::Discarded(()) => Some(entry),
        };
        (entry, if close { receive::Outcome::CloseAfter } else { receive::Outcome::Continue })
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Effect<'w, 'd, ID, A, S, E, X> for app::Resumed
where
    A: app::Resumable<'d, ID, Continuation = app::resume::Retained> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn apply<'entry, 'p, 'turn, 'driver, 'context>(
        self,
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Option<engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>>, receive::Outcome)
    where
        'd: 'p,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        match self {
            Self::Complete(done) => done.apply(context, entry, flow, turn, driver),
            Self::Paused(pause) => pause.apply(context, entry, flow, turn, driver),
        }
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Effect<'w, 'd, ID, A, S, E, X> for app::Pause
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn apply<'entry, 'p, 'turn, 'driver, 'context>(
        self,
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        mut entry: engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) -> (Option<engine::Entry<'entry, 'p, 'w, 'd, ID, A, E::Transport>>, receive::Outcome)
    where
        'd: 'p,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        let close = entry.get().state.closing.is_closing();
        if !close {
            entry.suspend();
            if let app::Pause::Yield = self {
                entry.get_mut().state.work.request_next();
                entry.pending().wake();
            }
        }
        let entry = match flow {
            flow::Flow::Continue(()) => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit_entry::<false, false, S, E, X>(
                    context, entry, turn, driver,
                )
            }
            flow::Flow::Discarded(()) => Some(entry),
        };
        (entry, if close { receive::Outcome::CloseAfter } else { receive::Outcome::Continue })
    }
}
