use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::{
        pool,
        slot::transmit::{self, outcome},
    },
    wire::{
        self,
        send::{self, reclaim},
    },
};
use engine::ops::close::{self, ClosePhase as _};

use crate::{
    app::env,
    client::{
        app::{self, request},
        attempt,
        close::{Directive, Reason},
        conn::{
            self, State, closing,
            engine::{
                self,
                liveness::{self, refresh},
            },
            flow,
        },
        side::mode::Mode,
    },
    timing,
};

mod deadline;
pub(in crate::client) use deadline::Deadline;

enum Binding {
    Unbound,
    Bound,
}

enum Step {
    Failed(reclaim::Advance),
    Quiescent(reclaim::Advance),
    Runnable(reclaim::Advance),
    Waiting,
}
pub(in crate::client) trait Policy<'w, 'd: 'w, ID: Identity + 'd, A>:
    request::Mode + Sized
where
    A: app::Receive<'d, ID>
        + app::Lifecycle<'w, 'd, ID>
        + app::Application<'d, ID, Requests = Self>,
{
    fn drain<'p, 'owner, S, E, X>(
        engine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>;
    fn follow<'p, 'machine, 'turn, 'driver, 'context, 'owner, S, E, X>(
        engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        follow: flow::Follow,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>;

    fn finish<'p, 'machine, 'turn, 'driver, 'context, 'owner, S, E, X>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let target = engine.as_ref().backoff();
        {
            let this = engine.as_mut().project();
            let Some(liveness::Egress {
                flights: slots,
                connection: mut slot,
                pending,
                ..
            }) = this.links.egress(key, target)
            else {
                return;
            };
            let done = slot.sending().finish(slots, driver);
            pending.wait_send(slot.send_status().is_waiting());
            let update = match done {
                transmit::Finalize::Armed(()) => Deadline::Ensure,
                transmit::Finalize::Settled | transmit::Finalize::Abandoned => Deadline::Cancel,
                transmit::Finalize::Rejected(failure) => {
                    slot.state.fail(conn::Transmit::Rejected(failure));
                    let error = this
                        .controller
                        .rejected(slot.state.owner.attempt(), failure.into_error());
                    slot.state.closing.apply(Directive::from_submit(error));
                    Deadline::Cancel
                }
            };
            if matches!(
                closing::Callback::new(&slot.state.closing),
                closing::Callback::Ready
            ) {
                update.apply(
                    slot,
                    driver.driver_ref().clock().now(),
                    <E::Timing as timing::Policy>::SEND_DEADLINE,
                );
            }
        }
        engine.maybe_close(key, turn, driver);
    }

    fn submit<'p, 'machine, 'turn, 'driver, 'context, 'owner, S, E, X>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let target = engine.as_ref().backoff();
        let follow = {
            let this = engine.as_mut().project();
            let Some(mut sender) = this.links.sender(key, target) else {
                return;
            };
            let liveness::Egress {
                connection: mut slot,
                pending: handle,
                queue: egress,
                ..
            } = sender.view();
            if !slot.is_connected() {
                return;
            }
            let binding = match State::active(&mut slot, |active| {
                this.app.prepare(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    driver,
                );
            }) {
                flow::Active::Ready(flow::Flow::Continue(())) => Binding::Bound,
                flow::Active::Opening => Binding::Unbound,
                flow::Active::Ready(flow::Flow::Discarded(())) => {
                    handle.wake();
                    return;
                }
                flow::Active::Sealed => return,
            };
            if slot.state.closing.is_closing() {
                handle.wake();
            }
            let armed = slot.send_status().is_inflight();
            let done = sender.submit(driver);
            let liveness::Egress {
                connection: mut slot,
                pending: handle,
                queue: egress,
                ..
            } = sender.view();
            let step = match done {
                transmit::Attempt::Runnable(advance) => {
                    handle.wake();
                    Step::Runnable(advance)
                }
                transmit::Attempt::Waiting => Step::Waiting,
                transmit::Attempt::Quiescent(advance) => Step::Quiescent(advance),
                transmit::Attempt::Invalid(error) => {
                    slot.state.fail(conn::Transmit::Invalid(error));
                    slot.state
                        .closing
                        .apply(Directive::reconnect(Reason::Transport));
                    handle.wake();
                    Step::Failed(reclaim::Advance::Idle)
                }
                transmit::Attempt::Rejected(failure, advance) => {
                    slot.state.fail(conn::Transmit::Rejected(failure));
                    let error = this
                        .controller
                        .rejected(slot.state.owner.attempt(), failure.into_error());
                    slot.state.closing.apply(Directive::from_submit(error));
                    handle.wake();
                    Step::Failed(advance)
                }
            };
            let status = slot.send_status();
            let live = !egress.is_empty()
                || status.is_inflight()
                || status.retention() == outcome::Retention::Held
                || status.is_closing();
            let update = match &step {
                Step::Failed(reclaim::Advance::Idle | reclaim::Advance::Ready) => Deadline::None,
                Step::Waiting
                | Step::Quiescent(reclaim::Advance::Idle | reclaim::Advance::Ready)
                | Step::Runnable(reclaim::Advance::Idle | reclaim::Advance::Ready)
                    if !live =>
                {
                    Deadline::Cancel
                }
                Step::Runnable(reclaim::Advance::Idle | reclaim::Advance::Ready)
                | Step::Quiescent(reclaim::Advance::Ready) => Deadline::Progress,
                Step::Waiting | Step::Quiescent(reclaim::Advance::Idle)
                    if !armed && status.is_inflight() =>
                {
                    Deadline::Progress
                }
                Step::Waiting | Step::Quiescent(reclaim::Advance::Idle) => Deadline::Ensure,
            };
            let follow = match (binding, step) {
                (
                    Binding::Unbound,
                    Step::Failed(reclaim::Advance::Idle | reclaim::Advance::Ready)
                    | Step::Quiescent(reclaim::Advance::Idle | reclaim::Advance::Ready)
                    | Step::Runnable(reclaim::Advance::Idle | reclaim::Advance::Ready)
                    | Step::Waiting,
                ) => flow::Follow::Unbound,
                (
                    Binding::Bound,
                    Step::Failed(reclaim::Advance::Ready)
                    | Step::Quiescent(reclaim::Advance::Ready)
                    | Step::Runnable(reclaim::Advance::Ready),
                ) => flow::Follow::Released,
                (
                    Binding::Bound,
                    Step::Failed(reclaim::Advance::Idle)
                    | Step::Quiescent(reclaim::Advance::Idle)
                    | Step::Runnable(reclaim::Advance::Idle)
                    | Step::Waiting,
                ) => flow::Follow::Bound,
            };
            update.apply(
                slot,
                driver.driver_ref().clock().now(),
                <E::Timing as timing::Policy>::SEND_DEADLINE,
            );
            follow
        };
        Self::follow::<S, E, X>(engine.as_mut(), key, follow, turn.reborrow(), driver);
        engine.maybe_close(key, turn, driver);
    }
}
impl<'w, 'd: 'w, ID: Identity + 'd, A> Policy<'w, 'd, ID, A> for request::Enabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Requests<'d, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
{
    fn drain<'p, 'owner, S, E, X>(
        mut engine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        let target = engine.as_ref().backoff();
        let this = engine.as_mut().project();
        let Some(liveness::Egress {
            connection: mut slot,
            pending: handle,
            queue,
            ..
        }) = this.links.egress(key, target)
        else {
            return;
        };
        let update = (|| {
            match slot.state.closing.status() {
                conn::CloseStatus::Closing(
                    conn::ClosePhase::Finalized | conn::ClosePhase::Claimed,
                    _,
                ) => {
                    return Deadline::None;
                }
                conn::CloseStatus::Open | conn::CloseStatus::Closing(..) => {}
            }
            let wake = slot.io().wake_target();
            let Some(conn) = slot.state.conn_mut() else {
                return Deadline::None;
            };
            let mut reqs = request::Turn::new(
                conn::Id::from_key(key),
                conn,
                queue.reborrow(),
                turn.reborrow().app(),
                wake,
                driver.driver().reborrow(),
            );
            this.app.drain(&mut reqs);
            let done = reqs.finish();
            let close = done.close();
            let flow = match close {
                None => flow::Flow::Continue(()),
                Some(rule) => match State::apply(&mut slot, rule) {
                    flow::Apply::Applied(flow) => flow,
                    flow::Apply::Sealed => {
                        handle.wake();
                        return Deadline::Cancel;
                    }
                },
            };
            if done.contains(request::Flag::WriteShutdown) {
                slot.shutdown();
                handle.wake();
            }
            if done.contains(request::Flag::Exhausted) {
                slot.state.work.drain();
                handle.wake();
            }
            if close.is_some() {
                handle.wake();
            }
            let cut = match flow {
                flow::Flow::Continue(()) => false,
                flow::Flow::Discarded(()) => true,
            };

            if cut {
                Deadline::Cancel
            } else {
                if done.contains(request::Flag::Queued) {
                    handle.wake();
                }
                let status = slot.send_status();
                if done.contains(request::Flag::Enqueued)
                    || done.contains(request::Flag::Waiting)
                    || done.contains(request::Flag::Exhausted)
                    || done.contains(request::Flag::Queued)
                    || status.is_inflight()
                    || status.retention() == outcome::Retention::Held
                    || status.is_closing()
                {
                    Deadline::Ensure
                } else {
                    Deadline::Cancel
                }
            }
        })();
        update.apply(
            slot,
            driver.driver_ref().clock().now(),
            <E::Timing as timing::Policy>::SEND_DEADLINE,
        );
    }

    fn follow<'p, 'machine, 'turn, 'driver, 'context, 'owner, S, E, X>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        follow: flow::Follow,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        let drain = match <<<A::Wire as wire::Spec>::Send as send::Policy<
            A::Wire,
        >>::Reclaim as reclaim::Policy>::direct()
        {
            reclaim::Direct::Independent => matches!(follow, flow::Follow::Released),
            reclaim::Direct::Retained => false,
        };
        if drain {
            <Self as Policy<'w, 'd, ID, A>>::drain(engine.as_mut(), key, turn, driver);
        }
        if matches!(follow, flow::Follow::Bound | flow::Follow::Released) {
            let now = driver.driver_ref().clock().now();
            refresh::run(engine.as_mut(), key, now, driver);
        }
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, A> Policy<'w, 'd, ID, A> for request::Disabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A: app::Application<'d, ID, Requests = request::Disabled>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
{
    fn drain<'p, 'owner, S, E, X>(
        _: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        _: pool::Key<'d, ID>,
        _: schedule::Turn<'_, 'd>,
        _: &mut retained::Context<'_, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
    }

    fn follow<'p, 'machine, 'turn, 'driver, 'context, 'owner, S, E, X>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        follow: flow::Follow,
        _: schedule::Turn<'turn, 'd>,
        driver: &'driver mut retained::Context<'context, 'owner, 'd>,
    ) where
        'd: 'owner,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        if matches!(follow, flow::Follow::Bound | flow::Follow::Released) {
            let now = driver.driver_ref().clock().now();
            refresh::run(engine.as_mut(), key, now, driver);
        }
    }
}
