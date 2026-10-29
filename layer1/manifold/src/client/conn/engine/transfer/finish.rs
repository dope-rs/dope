use std::{convert, pin};

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::link::pool;

use crate::{
    app::{env, receive::ingress},
    client::{
        app::{self, resume},
        attempt,
        conn::{
            engine::{
                self, liveness,
                ops::close,
                transfer::{effect::Effect as _, send},
            },
            flow,
        },
        side::mode::Mode,
    },
};

pub(in crate::client::conn::engine) trait Finish<'w, 'd: 'w, ID: Identity + 'd, A, C>
where
    A: app::Lifecycle<'w, 'd, ID>,
{
    fn finish<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<<A::Continuation as resume::Mode>::Outcome>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>;
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Finish<'w, 'd, ID, A, resume::Complete>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID, Continuation = resume::Complete> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    fn finish<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<<A::Continuation as resume::Mode>::Outcome>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        let (done, next) = flow.into_parts();
        ingress::Progress::Done(done.apply(self, key, next, turn, driver))
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Finish<'w, 'd, ID, A, resume::Retained>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Resumable<'d, ID, Continuation = resume::Retained> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
{
    fn finish<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        key: pool::Key<'d, ID>,
        flow: flow::Flow<<A::Continuation as resume::Mode>::Outcome>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) -> ingress::Progress<convert::Infallible>
    where
        'd: 'owner,
        A::Requests: send::Policy<'w, 'd, ID, A>,
    {
        let (done, next) = flow.into_parts();
        match done {
            app::Resumed::Complete(done) => {
                ingress::Progress::Done(done.apply(self, key, next, turn, driver))
            }
            app::Resumed::Paused(pause) => {
                ingress::Progress::Done(pause.apply(self, key, next, turn, driver))
            }
        }
    }
}
