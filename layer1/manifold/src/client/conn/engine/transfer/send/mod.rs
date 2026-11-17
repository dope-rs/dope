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
use engine::ops::close;

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

    fn drain<'p, T: dope_net::Transport>(
        _: &mut A,
        _: &mut engine::Sender<'_, 'p, 'w, 'd, ID, A, T>,
        _: schedule::Turn<'_, 'd>,
        _: &mut driver::Context<'_, 'd>,
    ) -> Deadline {
        Deadline::None
    }

    fn finish<'p, S, E, X>(
        mut engine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
    {
        let (pool, mut context) = engine.as_mut().parts();
        let Ok(mut entry) = pool.entry(key) else { return };
        {
            let Some(mut sender) = entry.sender() else { return };
            Self::finish_admitted::<S, E, X>(
                context.app, context.controller, &mut sender, context.liveness, context.target, driver,
            );
        }
        drop(<engine::Machine<'p, 'w, 'd, ID, A, S, E, X> as close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>>::maybe_entry(
            &mut context, entry, turn, driver,
        ));
    }

    fn finish_admitted<S, E, X>(
        app: &mut A,
        controller: &mut S,
        sender: &mut engine::Sender<'_, '_, 'w, 'd, ID, A, E::Transport>,
        liveness: &mut liveness::State<'d, ID, <E::Transport as dope_net::Transport>::Prepared, X>,
        target: schedule::ready::Target<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        let pool::EgressMut {
            flights,
            connection: slot,
            pending,
            ..
        } = sender.view();
        let key = slot.key();
        // SAFETY: the sender retains the original primary slot paired with this book.
        let source = unsafe { engine::raw::reunite(liveness, slot, key) };
        let mut slot = liveness::Change::new(source, target);
        let _ = State::poll_close(&mut slot, app);
        let done = slot.sending().finish(flights, driver);
        pending.wait_send(slot.send_status().is_waiting());
        let update = match done {
            transmit::Finalize::Armed(()) => Deadline::Ensure,
            transmit::Finalize::Settled | transmit::Finalize::Abandoned => Deadline::Cancel,
            transmit::Finalize::Rejected(failure) => {
                slot.state.fail(conn::Transmit::Rejected(failure));
                let error = controller.rejected(slot.state.owner.attempt(), failure.into_error());
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

    fn submit<'p, const SENT: bool, const DRAIN: bool, S, E, X>(
        mut engine: pin::Pin<&mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        A::Continuation: liveness::Inspection<'d, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: Mode<'d, ID>
            + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
    {
        let (pool, mut context) = engine.as_mut().parts();
        if let Ok(entry) = pool.entry(key) {
            drop(Self::submit_entry::<SENT, DRAIN, S, E, X>(
                &mut context, entry, turn, driver,
            ));
        }
    }

    fn submit_entry<'a, 'p, const SENT: bool, const DRAIN: bool, S, E, X>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        mut entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>>
    where
        A::Continuation: liveness::Inspection<'d, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>,
    {
        let queued = entry.has_queued();
        match entry.sender() {
            Some(mut sender) => Self::submit_admitted::<SENT, DRAIN, S, E, X>(
                context.app, context.controller, &mut sender,
                context.liveness, context.target, queued, turn.reborrow(), driver,
            ),
            None => { let _ = State::poll_close(entry.get_mut(), context.app); }
        }
        <engine::Machine<'p, 'w, 'd, ID, A, S, E, X> as close::ClosePhase<'p, 'w, 'd, ID, A, S, E, X>>::maybe_entry(
            context, entry, turn, driver,
        )
    }

    fn submit_admitted<const SENT: bool, const DRAIN: bool, S, E, X>(
        app: &mut A,
        controller: &mut S,
        sender: &mut engine::Sender<'_, '_, 'w, 'd, ID, A, E::Transport>,
        liveness: &mut liveness::State<'d, ID, <E::Transport as dope_net::Transport>::Prepared, X>,
        target: schedule::ready::Target<'d>,
        queued: bool,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) where
        A::Continuation: liveness::Inspection<'d, ID, A>,
        S: attempt::Control<'d, E::Transport, ID>,
        E: env::Spec<Wire = A::Wire>,
        E::Transport: dope_net::Transport,
        X: liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    {
        let key = sender.view().connection.key();
        'submit: {
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
                        app.sent(conn::Id::from_key(key), pending, driver.state_token());
                        if slot.state.conn_mut().is_some_and(|conn| {
                            <A::Continuation as liveness::Inspection<'d, ID, A>>::wake(app, conn)
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
                    let update = Self::drain(app, sender, turn.reborrow(), driver);
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
                let _ = State::poll_close(slot, app);
                break 'submit;
            }
            if !slot.is_connected() {
                break 'submit;
            }
            let active = State::active(slot, |active, wire| {
                app.prepare(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    queued,
                    egress.reborrow(),
                    driver,
                    wire,
                );
            });
            match State::poll_close(slot, app) {
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
            let done = sender.submit(driver, drop);
            let pool::EgressMut {
                connection: slot,
                pending: handle,
                queue: egress,
                ..
            } = sender.view();
            let status = slot.send_status();
            let live = slot.state.work.has_requests()
                || !egress.is_empty()
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
                    let error =
                        controller.rejected(slot.state.owner.attempt(), failure.into_error());
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
        let update = Self::drain(app, sender, turn.reborrow(), driver);
        let slot = sender.view().connection;
        // SAFETY: submission preserves this borrowed slot and its paired book.
        let source = unsafe { engine::raw::reunite(liveness, slot, key) };
        update.apply(
            liveness::Change::new(source, target),
            driver.driver_ref().clock().now(),
            <E::Timing as timing::Policy>::SEND_DEADLINE,
        );
    }

            if bound {
                let slot = sender.view().connection;
                // SAFETY: the same admitted sender retains this book's primary slot.
                let source = unsafe { engine::raw::reunite(liveness, &*slot, key) };
                refresh::inspect::<ID, A, E, X>(
                    app,
                    liveness::Change::new(source, target),
                    driver.driver_ref().clock().now(),
                    driver,
                );
            }
        }
    }
}
impl<'w, 'd: 'w, ID: Identity + 'd, A> Policy<'w, 'd, ID, A> for request::Enabled
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID> + app::Requests<'d, ID>,
    A::Continuation: liveness::Inspection<'d, ID, A>,
{
    const ENABLED: bool = true;

    fn drain<'p, T: dope_net::Transport>(
        app: &mut A,
        sender: &mut engine::Sender<'_, 'p, 'w, 'd, ID, A, T>,
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
        if <A::Continuation as liveness::Inspection<'d, ID, A>>::RESUMABLE
            && slot.state.work.resume_first()
            && !slot.is_discarded()
        {
            slot.state.work.drain();
            handle.wake();
            return Deadline::None;
        }
        let Some(conn) = slot.state.conn_mut() else {
            return Deadline::None;
        };
        let before = if <A::Continuation as liveness::Inspection<'d, ID, A>>::RESUMABLE {
            turn.reborrow().app().remaining()
        } else {
            0
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
        let served = <A::Continuation as liveness::Inspection<'d, ID, A>>::RESUMABLE
            && turn.reborrow().app().remaining() < before;
        slot.state.work.drained(
            done.contains(request::Flag::Waiting) || done.contains(request::Flag::Exhausted),
            served,
        );
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
        if slot.state.work.has_requests()
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
