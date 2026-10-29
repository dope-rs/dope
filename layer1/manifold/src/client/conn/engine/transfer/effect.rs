use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::link::pool::{self, pending};

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

pub(in crate::client::conn::engine) trait Effect<'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    fn apply<'machine, 'turn, 'driver, 'context, 'owner>(
        self,
        engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> receive::Outcome
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>;
}

impl<'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X> Effect<'p, 'w, 'd, ID, A, S, E, X>
    for app::ChunkOutcome
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    fn apply<'machine, 'turn, 'driver, 'context, 'owner>(
        self,
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> receive::Outcome
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        if let Err(error) = self.into_result() {
            return receive::Outcome::Failed(error);
        }
        let close = {
            let this = engine.as_mut().project();
            match this.links.pool_mut().get_mut(key) {
                Some(slot) => slot.state.closing.is_closing(),
                None => false,
            }
        };
        match flow {
            flow::Flow::Continue(()) => <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(
                engine.as_mut(),
                key,
                turn.reborrow(),
                driver,
            ),
            flow::Flow::Discarded(()) => {}
        }
        if close {
            receive::Outcome::CloseAfter
        } else {
            receive::Outcome::Continue
        }
    }
}

impl<'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X> Effect<'p, 'w, 'd, ID, A, S, E, X>
    for app::Pause
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    fn apply<'machine, 'turn, 'driver, 'context, 'owner>(
        self,
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<()>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> receive::Outcome
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        let close = {
            let this = engine.as_mut().project();
            let close = match this.links.pool_mut().get_mut(key) {
                Some(slot) => slot.state.closing.is_closing(),
                None => false,
            };
            if !close {
                this.links.pool_mut().suspend(key);
                match self {
                    app::Pause::Yield => {
                        if let Some((slot, handle)) =
                            pending::Mut::of(this.links.pool_mut()).get(key)
                        {
                            slot.state.work.request_next();
                            handle.wake();
                        }
                    }
                    app::Pause::Blocked => {}
                }
            }
            close
        };
        match flow {
            flow::Flow::Continue(()) => {
                <A::Requests as send::Policy<'w, 'd, ID, A>>::submit(engine, key, turn, driver)
            }
            flow::Flow::Discarded(()) => {}
        }
        if close {
            receive::Outcome::CloseAfter
        } else {
            receive::Outcome::Continue
        }
    }
}
