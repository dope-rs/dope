use std::pin;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::{
    link::{
        egress, pool,
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

pub(in crate::client) trait Policy<'w, 'd: 'w, ID: Identity + 'd, A>:
    request::Mode + Sized
where
    A: app::Receive<'d, ID>
        + app::Lifecycle<'w, 'd, ID>
        + app::Application<'d, ID, Requests = Self>,
{
    const ENABLED: bool = false;

    fn drain<'p>(
        _: &mut A,
        _: &mut engine::Sender<'_, 'p, 'w, 'd, ID, A>,
        _: schedule::Turn<'_, 'd>,
        _: &mut driver::Context<'_, 'd>,
    ) -> Deadline {
        Deadline::None
    }

    fn finish<'p, 'machine, 'turn, 'driver, 'context, S, E, X>(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) where
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
            let _ = State::poll_close(&mut slot, this.app);
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

    fn submit<
        'p,
        'machine,
        'turn,
        'driver,
        'context,
        const SENT: bool,
        const DRAIN: bool,
        S,
        E,
        X,
    >(
        mut engine: pin::Pin<&'machine mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &'driver mut driver::Context<'context, 'd>,
    ) where
        A::Continuation: liveness::Inspection<'d, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'d, ID, A, S, E>,
    {
        let target = engine.as_ref().backoff();
        'submit: {
            let follow = {
                let this = engine.as_mut().project();
                let (pool, liveness) = this.links.parts();
                let Some((mut sender, queued)) = pool.sender_with_recv(key) else {
                    if let Some(slot) = pool.get_mut(key) {
                        let _ = State::poll_close(slot, this.app);
                    }
                    break 'submit;
                };
                {
                    let pool::EgressMut {
                        connection: slot,
                        pending: handle,
                        queue,
                        ..
                    } = sender.view();
                    let drain = if SENT {
                        if slot.state.is_attached() {
                            let pending = if queue.is_empty() && !slot.send_status().is_closing() {
                                egress::Pending::Absent
                            } else {
                                egress::Pending::Present
                            };
                            this.app
                                .sent(conn::Id::from_key(key), pending, driver.state_token());
                            if slot.state.conn_mut().is_some_and(|conn| {
                                <A::Continuation as liveness::Inspection<'d, ID, A>>::wake(
                                    this.app, conn,
                                )
                            }) {
                                if !slot.is_closing() {
                                    slot.state.work.request_next();
                                }
                                handle.wake();
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        DRAIN
                    };
                    if drain && Self::ENABLED {
                        let update = Self::drain(this.app, &mut sender, turn.reborrow(), driver);
                        let slot = sender.view().connection;
                        // SAFETY: the sender retains this pool's exact slot and paired book.
                        let source = unsafe { engine::raw::reunite(liveness, slot, key) };
                        update.apply(
                            liveness::Change::new(source, target),
                            driver.driver_ref().clock().now(),
                            <E::Timing as timing::Policy>::SEND_DEADLINE,
                        );
                    }
                }
                let pool::EgressMut {
                    connection: slot,
                    pending: handle,
                    queue: egress,
                    ..
                } = sender.view();
                if slot.is_closing() {
                    let _ = State::poll_close(slot, this.app);
                    break 'submit;
                }
                if !slot.is_connected() {
                    break 'submit;
                }
                let active = State::active(slot, |active, wire| {
                    this.app.prepare(
                        conn::Ctx::new(active, turn.reborrow().app()),
                        queued,
                        egress.reborrow(),
                        driver,
                        wire,
                    );
                });
                match State::poll_close(slot, this.app) {
                    flow::Apply::Applied(flow::Flow::Continue(())) => {}
                    flow::Apply::Applied(flow::Flow::Discarded(())) | flow::Apply::Sealed => {
                        break 'submit;
                    }
                }
                let bound = match active {
                    flow::Active::Ready(flow::Flow::Continue(())) => true,
                    flow::Active::Opening => false,
                    flow::Active::Ready(flow::Flow::Discarded(())) => {
                        handle.wake();
                        break 'submit;
                    }
                    flow::Active::Sealed => break 'submit,
                };
                if slot.state.closing.is_closing() {
                    handle.wake();
                }
                let armed = slot.send_status().is_inflight();
                let done = sender.submit(driver);
                let pool::EgressMut {
                    connection: slot,
                    pending: handle,
                    queue: egress,
                    ..
                } = sender.view();
                let status = slot.send_status();
                let live = !egress.is_empty()
                    || status.is_inflight()
                    || status.retention() == outcome::Retention::Held
                    || status.is_closing();
                let (advance, update) = match done {
                    transmit::Attempt::Runnable(advance) => {
                        handle.wake();
                        (
                            advance,
                            if live {
                                Deadline::Progress
                            } else {
                                Deadline::Cancel
                            },
                        )
                    }
                    transmit::Attempt::Quiescent(reclaim::Advance::Ready) => (
                        reclaim::Advance::Ready,
                        if live {
                            Deadline::Progress
                        } else {
                            Deadline::Cancel
                        },
                    ),
                    transmit::Attempt::Waiting
                    | transmit::Attempt::Quiescent(reclaim::Advance::Idle) => (
                        reclaim::Advance::Idle,
                        if !live {
                            Deadline::Cancel
                        } else if !armed && status.is_inflight() {
                            Deadline::Progress
                        } else {
                            Deadline::Ensure
                        },
                    ),
                    transmit::Attempt::Invalid(error) => {
                        slot.state.fail(conn::Transmit::Invalid(error));
                        slot.state
                            .closing
                            .apply(Directive::reconnect(Reason::Transport));
                        handle.wake();
                        (reclaim::Advance::Idle, Deadline::None)
                    }
                    transmit::Attempt::Rejected(failure, advance) => {
                        slot.state.fail(conn::Transmit::Rejected(failure));
                        let error = this
                            .controller
                            .rejected(slot.state.owner.attempt(), failure.into_error());
                        slot.state.closing.apply(Directive::from_submit(error));
                        handle.wake();
                        (advance, Deadline::None)
                    }
                };
                // SAFETY: the native sender retains this key's slot from the paired pool.
                let source = unsafe { engine::raw::reunite(liveness, slot, key) };
                update.apply(
                    liveness::Change::new(source, target),
                    driver.driver_ref().clock().now(),
                    <E::Timing as timing::Policy>::SEND_DEADLINE,
                );
                if bound && Self::ENABLED
                    && <<<A::Wire as wire::Spec>::Send as send::Policy<A::Wire>>::Reclaim as reclaim::Policy>::direct()
                        == reclaim::Direct::Independent
                    && matches!(advance, reclaim::Advance::Ready)
                {
                    let update = Self::drain(this.app, &mut sender, turn.reborrow(), driver);
                    let slot = sender.view().connection;
                    // SAFETY: submission preserves this borrowed slot and its paired book.
                    let source = unsafe { engine::raw::reunite(liveness, slot, key) };
                    update.apply(
                        liveness::Change::new(source, target),
                        driver.driver_ref().clock().now(),
                        <E::Timing as timing::Policy>::SEND_DEADLINE,
                    );
                }
                bound
            };
            if follow {
                let now = driver.driver_ref().clock().now();
                refresh::run(engine.as_mut(), key, now, driver);
            }
        }
        engine.maybe_close(key, turn, driver);
    }
}
impl<'w, 'd: 'w, ID: Identity + 'd, A> Policy<'w, 'd, ID, A> for request::Enabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Requests<'d, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
{
    const ENABLED: bool = true;

    fn drain<'p>(
        app: &mut A,
        sender: &mut engine::Sender<'_, 'p, 'w, 'd, ID, A>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Deadline {
        let pool::EgressMut {
            connection: slot,
            pending: handle,
            queue,
            ..
        } = sender.view();
        let key = slot.key();
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
            driver.reborrow(),
        );
        app.drain(&mut reqs);
        let (done, close) = reqs.finish();
        let queued = !queue.is_empty();
        let flow = match close {
            None => flow::Flow::Continue(()),
            Some(rule) => match State::apply(slot, rule) {
                flow::Apply::Applied(flow) => flow,
                flow::Apply::Sealed => {
                    handle.wake();
                    return Deadline::Cancel;
                }
            },
        };
        if done.contains(request::Flag::WriteShutdown) {
            slot.shutdown();
        }
        if done.contains(request::Flag::Exhausted) {
            slot.state.work.drain();
        }
        if close.is_some()
            || done.contains(request::Flag::WriteShutdown)
            || done.contains(request::Flag::Exhausted)
            || queued
        {
            handle.wake();
        }
        if let flow::Flow::Discarded(()) = flow {
            return Deadline::Cancel;
        }
        let status = slot.send_status();
        if done.contains(request::Flag::Waiting)
            || done.contains(request::Flag::Exhausted)
            || queued
            || status.is_inflight()
            || status.retention() == outcome::Retention::Held
            || status.is_closing()
        {
            Deadline::Ensure
        } else {
            Deadline::Cancel
        }
    }
}

impl<'w, 'd: 'w, ID: Identity + 'd, A> Policy<'w, 'd, ID, A> for request::Disabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    A: app::Application<'d, ID, Requests = request::Disabled>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
{
}
