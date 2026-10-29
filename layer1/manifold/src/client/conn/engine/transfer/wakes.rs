use std::pin;

use dope_core::{
    driver::{retained, route, route::Identity, schedule},
    io::event::net::send,
};
use dope_net::link::{
    egress,
    event::SendOutcome,
    pool::{
        self, pending,
        recv::{input, reserve::Choice},
    },
};

use crate::{
    app::{dispatch, env, receive::ingress},
    client::{
        app::{self, resume},
        attempt,
        close::{Directive, Reason},
        conn::{
            self, closing,
            engine::{
                self, drive, liveness,
                ops::{
                    close::{self, ClosePhase as _},
                    connect,
                },
                transfer::send::Policy,
            },
        },
        deadline,
        side::mode::Mode,
    },
};

pub(in crate::client::conn::engine) trait Activation<'d, ID: Identity + 'd, C> {
    fn activate<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut dispatch::raw::Context<'context, 'owner, 'd, dispatch::raw::Retained>,
    ) where
        'd: 'owner;
}

pub(in crate::client) trait Settlement<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>:
    Sized
where
    A: app::Receive<'d, ID, Continuation = Self> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    fn sent<'machine, 'turn, 'driver, 'context, 'owner>(
        engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>;

    /// Re-submits this key's request policy and re-checks the close condition.
    fn settle<'machine, 'turn, 'driver, 'context, 'owner>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
        drain: bool,
    ) where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
        X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        if drain {
            <A::Requests as Policy<'w, 'd, ID, A>>::drain(
                engine.as_mut(),
                key,
                turn.reborrow(),
                driver,
            );
        }
        <A::Requests as Policy<'w, 'd, ID, A>>::submit(
            engine.as_mut(),
            key,
            turn.reborrow(),
            driver,
        );
        engine.as_mut().maybe_close(key, turn, driver);
    }
}

pub(in crate::client) trait Completion<'d> {
    fn complete<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        done: send::Completion<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner;
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Completion<'d>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    A::Continuation: Settlement<'p, 'w, 'd, ID, A, S, E, X>,
    Self: close::ClosePhase<'d, ID, A, S, E>,
{
    fn complete<'machine, 'turn, 'driver, 'context, 'owner>(
        mut self: pin::Pin<&'machine mut Self>,
        done: send::Completion<'d>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
    {
        let mut engine = self.as_mut();
        let target = engine.as_ref().backoff();
        let result = {
            let this = engine.as_mut().project();
            this.links.settle(
                done,
                driver,
                |slot, outcome| {
                    match outcome {
                        SendOutcome::Terminal { .. } => {}
                        SendOutcome::Queued { .. } | SendOutcome::WriteShutdown { .. }
                            if slot.state.is_attached() => {}
                        _ => return,
                    }
                    if matches!(
                        closing::Callback::new(&slot.state.closing),
                        closing::Callback::Ready
                    ) {
                        slot.cancel(deadline::Kind::Send);
                    }
                },
                target,
            )
        };
        let key = match result {
            SendOutcome::Queued { key, .. } => key,
            SendOutcome::WriteShutdown { key } => key,
            SendOutcome::Terminal { key } => {
                <A::Requests as Policy<'w, 'd, ID, A>>::finish(
                    engine.as_mut(),
                    key,
                    turn.reborrow(),
                    driver,
                );
                return;
            }
            SendOutcome::Rejected { key, failure } => {
                let this = engine.as_mut().project();
                let Some(slot) = this.links.pool_mut().get_mut(key) else {
                    return;
                };
                slot.state.fail(conn::Transmit::Rejected(failure));
                let error = this
                    .controller
                    .rejected(slot.state.owner.attempt(), failure.into_error());
                let rule = Directive::from_submit(error);
                slot.state.closing.apply(rule);
                return engine.as_mut().close_slot(key, rule.reason(), turn, driver);
            }
            SendOutcome::Failed { key, failure } => {
                let this = engine.as_mut().project();
                let Some(slot) = this.links.pool_mut().get_mut(key) else {
                    return;
                };
                slot.state.fail(conn::Transmit::Io(failure));
                return engine
                    .as_mut()
                    .close_slot(key, Reason::Transport, turn, driver);
            }
            SendOutcome::Invalid { key, error } => {
                let this = engine.as_mut().project();
                let Some(slot) = this.links.pool_mut().get_mut(key) else {
                    return;
                };
                slot.state.fail(conn::Transmit::Invalid(error));
                return engine
                    .as_mut()
                    .close_slot(key, Reason::Transport, turn, driver);
            }
            SendOutcome::Close(key) | SendOutcome::CloseSent { key, .. } => {
                return engine
                    .as_mut()
                    .close_slot(key, Reason::Transport, turn, driver);
            }
            SendOutcome::Direct { key, .. } => {
                return engine
                    .as_mut()
                    .close_slot(key, Reason::Transport, turn, driver);
            }
            SendOutcome::Drop => return,
        };
        <A::Continuation as Settlement<'p, 'w, 'd, ID, A, S, E, X>>::sent(
            engine, key, turn, driver,
        );
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Activation<'d, ID, resume::Complete>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID, Continuation = resume::Complete>
        + app::Lifecycle<'w, 'd, ID>
        + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: ingress::Policy<'p, 'w, 'd, ID, Input = A::Input, Wire = A::Wire>
        + drive::Run<'d>
        + close::ClosePhase<'d, ID, A, S, E>
        + connect::Connect<'d, ID, A, S, E>,
{
    fn activate<'machine, 'turn, 'driver, 'context, 'owner>(
        self: pin::Pin<&'machine mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut dispatch::raw::Context<'context, 'owner, 'd, dispatch::raw::Retained>,
    ) where
        'd: 'owner,
    {
        let key = self
            .as_ref()
            .project_ref()
            .links
            .pool()
            .inspect()
            .by_target(target);
        apply(self, key, turn, driver);
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Activation<'d, ID, resume::Retained>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Resumable<'d, ID, Continuation = resume::Retained>
        + app::Lifecycle<'w, 'd, ID>
        + app::Scheduling<'d, ID>,
    A::Input: app::Policy<'w, 'd, ID, A> + ingress::Dispatch<A::Wire>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    <A::Input as input::Mode<A::Wire>>::Reserve:
        Choice<A::Wire, <A::Input as input::Mode<A::Wire>>::Ingress>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: ingress::Policy<'p, 'w, 'd, ID, Input = A::Input, Wire = A::Wire>
        + drive::Run<'d>
        + close::ClosePhase<'d, ID, A, S, E>
        + connect::Connect<'d, ID, A, S, E>,
{
    fn activate<'machine, 'turn, 'driver, 'context, 'owner>(
        mut self: pin::Pin<&'machine mut Self>,
        target: route::Target<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut dispatch::raw::Context<'context, 'owner, 'd, dispatch::raw::Retained>,
    ) where
        'd: 'owner,
    {
        let key = {
            let this = self.as_mut().project();
            this.links.pool_mut().update_target(target, |slot| {
                let ready = slot.state.is_attached()
                    && match slot.state.conn_mut() {
                        Some(conn) => {
                            matches!(app::Resumable::wake(this.app, conn), app::Wake::Ready)
                        }
                        None => false,
                    };
                if ready && !slot.is_closing() {
                    slot.state.work.request_next();
                }
            })
        };
        apply(self, key, turn, driver);
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Settlement<'p, 'w, 'd, ID, A, S, E, X>
    for resume::Complete
where
    A: app::Receive<'d, ID, Continuation = resume::Complete> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn sent<'machine, 'turn, 'driver, 'context, 'owner>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let mut drain = false;
        {
            let this = engine.as_mut().project();
            if let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: _,
                queue,
            }) = this.links.pool_mut().egress_mut(key)
                && slot.state.is_attached()
            {
                let pending = match (queue.is_empty(), slot.send_status().is_closing()) {
                    (true, false) => egress::Pending::Absent,
                    (false, false | true) | (true, true) => egress::Pending::Present,
                };
                this.app
                    .sent(conn::Id::from_key(key), pending, driver.state_token());
                drain = true;
            }
        }
        Self::settle(engine, key, turn, driver, drain);
    }
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> Settlement<'p, 'w, 'd, ID, A, S, E, X>
    for resume::Retained
where
    A: app::Resumable<'d, ID, Continuation = resume::Retained> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    fn sent<'machine, 'turn, 'driver, 'context, 'owner>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let mut drain = false;
        {
            let this = engine.as_mut().project();
            if let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                pending: handle,
                queue,
            }) = this.links.pool_mut().egress_mut(key)
                && slot.state.is_attached()
            {
                let pending = match (queue.is_empty(), slot.send_status().is_closing()) {
                    (true, false) => egress::Pending::Absent,
                    (false, false | true) | (true, true) => egress::Pending::Present,
                };
                this.app
                    .sent(conn::Id::from_key(key), pending, driver.state_token());
                let wake = match slot.state.conn_mut() {
                    Some(conn) => {
                        matches!(app::Resumable::wake(this.app, conn), app::Wake::Ready)
                    }
                    None => false,
                };
                if wake {
                    if !slot.is_closing() {
                        slot.state.work.request_next();
                    }
                    handle.wake();
                }
                drain = true;
            }
        }
        Self::settle(engine, key, turn, driver, drain);
    }
}

fn apply<'p, 'w, 'd, 'machine, 'turn, 'driver, 'context, 'owner, ID: Identity + 'd, A, S, E, X>(
    mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    key: Option<pool::Key<'d, ID>>,
    turn: schedule::Turn<'turn, 'd>,
    driver: &'driver mut retained::Context<'context, 'owner, 'd>,
) where
    'd: 'owner + 'w,
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A::Requests: Policy<'w, 'd, ID, A>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    engine::Machine<'p, 'w, 'd, ID, A, S, E, X>:
        drive::Run<'d> + close::ClosePhase<'d, ID, A, S, E>,
{
    let Some(key) = key else {
        return drive::Run::run(engine, turn, driver);
    };
    {
        let this = engine.as_ref().project_ref();
        if let Some(handle) = pending::Pending::of(this.links.pool()).at(key) {
            handle.wake();
        }
    }
    <A::Requests as Policy<'w, 'd, ID, A>>::drain(engine.as_mut(), key, turn.reborrow(), driver);
    <A::Requests as Policy<'w, 'd, ID, A>>::submit(engine.as_mut(), key, turn.reborrow(), driver);
    drive::Run::run(engine, turn, driver);
}
