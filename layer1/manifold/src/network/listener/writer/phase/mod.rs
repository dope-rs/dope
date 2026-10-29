use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::{
    link::{
        event,
        pool::{self, pending},
        slot::{
            transmit::{self, outcome::Retention},
            types::{self, Readiness},
        },
    },
    wire::send::reclaim,
};

use crate::{
    app::{env, receive},
    network::listener::{
        self, conn, handler,
        runtime::control,
        writer::{self, flow, state},
    },
};

pub(in crate::network::listener) mod send;
mod sender;
pub(in crate::network::listener::writer) use sender::Sender;

#[derive(Clone, Copy)]
pub(in crate::network::listener) enum Wake {
    Pending,
    Capacity,
    Ingress,
    Progress,
    Send,
}

pub(in crate::network::listener) fn flush_dirty<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    enum Demand {
        Egress,
        Terminal,
        Close,
        Idle,
    }

    let round = match pending::Pending::of(root.as_ref().project_ref().owner.pool()).try_round() {
        Ok(round) => round,
        Err(pending::Busy) => return,
    };
    round.run(turn.reborrow().maint(), |key| {
        let capacity = pending::Pending::of(root.as_ref().project_ref().owner.pool())
            .at(key)
            .is_some_and(pending::Handle::has_capacity);
        let demand = {
            use flow::SlotFlow as _;

            let this = root.as_ref().project_ref();
            let view = this.owner.pool().inspect();
            let Some((slot, pending)) = view.egress(key) else {
                return pending::Disposition::Gone;
            };
            let sending = slot.send_status();
            let waiting = sending.is_waiting();
            let ready = capacity
                || (waiting && sending.retention() == Retention::Held)
                || (!waiting
                    && matches!(slot.flow(pending), flow::Flow::Plain | flow::Flow::Deferred));
            match slot.demand() {
                types::Demand::Discard | types::Demand::Close => Demand::Close,
                types::Demand::Terminal => Demand::Terminal,
                types::Demand::Continue if ready => Demand::Egress,
                types::Demand::Continue | types::Demand::Barrier => Demand::Idle,
            }
        };
        match demand {
            Demand::Egress => {
                let wake = if capacity {
                    Wake::Capacity
                } else {
                    Wake::Pending
                };
                commit_chunk(root.as_mut(), key, wake, turn.reborrow(), driver);
            }
            Demand::Terminal => commit_end(root.as_mut(), key, turn.reborrow(), driver),
            Demand::Close => control::close_slot(root.as_mut(), key, turn.reborrow(), driver),
            Demand::Idle => close_ready(root.as_mut(), key, turn.reborrow(), driver),
        }
        let this = root.as_ref().project_ref();
        if this.owner.pool().inspect().get(key).is_some() {
            pending::Disposition::Idle
        } else {
            pending::Disposition::Gone
        }
    });
}

pub(in crate::network::listener) fn commit_end<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let closed = {
        let this = root.as_mut().project();
        let Some(writer::Egress {
            flights: slots,
            connection: mut slot,
            pending,
            ..
        }) = this.owner.egress_mut(key)
        else {
            return;
        };
        let finish = slot.sending().finish(slots, driver);
        pending.wait_send(slot.send_status().is_waiting());
        let closed = match &finish {
            transmit::Finalize::Rejected(failure) => {
                slot.state.send.fail(conn::Transmit::Rejected(*failure))
            }
            transmit::Finalize::Armed(())
            | transmit::Finalize::Settled
            | transmit::Finalize::Abandoned => None,
        };
        match finish {
            transmit::Finalize::Armed(()) => {
                slot.ensure_send(driver.driver_ref().clock().now());
            }
            transmit::Finalize::Settled | transmit::Finalize::Abandoned => {
                slot.cancel_send();
            }
            transmit::Finalize::Rejected(_) => slot.cancel_send(),
        }
        closed
    };
    if let Some(egress) = closed {
        writer::release_egress(
            root.as_mut().project().app,
            key,
            egress,
            handler::Disposition::Closed,
        );
    }

    close_ready(root, key, turn, driver);
}

pub(in crate::network::listener) fn commit_chunk<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    wake: Wake,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let result = {
        let mut this = root.as_mut().project();
        let Some(mut sender) = this.owner.sender(key) else {
            return;
        };
        let writer::Egress {
            flights: slots,
            connection: mut slot,
            pending: handle,
            queue: egress,
            ..
        } = sender.view();
        match wake {
            Wake::Progress => {
                slot.progress(driver.driver_ref().clock().now());
            }
            Wake::Pending | Wake::Capacity => {
                slot.ensure_send(driver.driver_ref().clock().now());
            }
            Wake::Ingress | Wake::Send => {}
        }
        let result = if slot.is_closing() {
            flow::Handoff::Clear
        } else {
            let pending = egress.pending();
            let flow = flow::SlotFlow::flow(&*slot, pending);
            let flow = match (flow, wake) {
                (
                    flow::Flow::Stalled,
                    Wake::Capacity | Wake::Ingress | Wake::Progress | Wake::Send,
                ) => flow::Flow::Plain,
                (flow, _) => flow,
            };
            match flow {
                flow::Flow::Inflight => flow::Handoff::Armed,
                flow::Flow::Plain => {
                    flow::SlotFlow::advance_send(&mut *slot, slots, handle, driver);
                    let pending = egress.pending();
                    flow::SlotFlow::after_send(&mut *slot, pending)
                }
                flow::Flow::Stalled | flow::Flow::Held | flow::Flow::Clear => {
                    use transmit::outcome;

                    let mode = slot.state.send.mode();
                    let progress = match &mode {
                        state::Mode::Queued => slot.sending().flush_queue(slots, driver),
                        state::Mode::Direct => slot.sending().flush_direct(slots, driver),
                    };
                    handle.wait_send(slot.send_status().is_waiting());
                    match progress {
                        outcome::Progress::Submitted | outcome::Progress::Idle => {
                            let pending = egress.pending();
                            flow::SlotFlow::after_send(&mut *slot, pending)
                        }
                        outcome::Progress::Rejected(error) => {
                            slot.sending().discard();
                            let failure = match mode {
                                state::Mode::Queued => event::SendFailure::Queued(error),
                                state::Mode::Direct => event::SendFailure::Direct(error),
                            };
                            let closed = slot.state.send.fail(conn::Transmit::Rejected(failure));
                            if let Some(egress) = closed {
                                writer::release_egress(
                                    this.app.as_mut(),
                                    key,
                                    egress,
                                    handler::Disposition::Closed,
                                );
                            }
                            flow::Handoff::Clear
                        }
                    }
                }
                flow::Flow::Deferred => {
                    let done = sender.submit(driver);
                    let writer::Egress {
                        connection: mut slot,
                        ..
                    } = sender.view();
                    match done {
                        transmit::Attempt::Runnable(
                            reclaim::Advance::Idle | reclaim::Advance::Ready,
                        ) => flow::Handoff::Restage,
                        transmit::Attempt::Waiting => {
                            if slot.send_status().is_inflight() {
                                flow::Handoff::Armed
                            } else {
                                flow::Handoff::Clear
                            }
                        }
                        transmit::Attempt::Quiescent(
                            reclaim::Advance::Idle | reclaim::Advance::Ready,
                        ) => flow::Handoff::Clear,
                        transmit::Attempt::Invalid(error) => {
                            let closed = slot.state.send.fail(conn::Transmit::Invalid(error));
                            if let Some(egress) = closed {
                                writer::release_egress(
                                    this.app.as_mut(),
                                    key,
                                    egress,
                                    handler::Disposition::Closed,
                                );
                            }
                            flow::Handoff::Clear
                        }
                        transmit::Attempt::Rejected(
                            failure,
                            reclaim::Advance::Idle | reclaim::Advance::Ready,
                        ) => {
                            let closed = slot.state.send.fail(conn::Transmit::Rejected(failure));
                            if let Some(egress) = closed {
                                writer::release_egress(
                                    this.app.as_mut(),
                                    key,
                                    egress,
                                    handler::Disposition::Closed,
                                );
                            }
                            flow::Handoff::Clear
                        }
                    }
                }
            }
        };
        if matches!(result, flow::Handoff::Armed) && !matches!(wake, Wake::Pending | Wake::Capacity)
        {
            let writer::Egress {
                connection: mut slot,
                ..
            } = sender.view();
            slot.ensure_send(driver.driver_ref().clock().now());
        }
        result
    };
    match result {
        flow::Handoff::Clear => {}
        flow::Handoff::Armed => {}
        flow::Handoff::Restage => {
            let this = root.as_mut().project();
            if let Some(handle) = pending::Pending::of(this.owner.pool()).at(key) {
                handle.wake();
            }
        }
    }
    close_ready(root, key, turn, driver);
}

pub(in crate::network::listener) fn close_ready<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut retained::Context<'_, '_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    enum Step {
        Close,
        Retry,
        Idle,
    }
    enum Gate {
        None,
        Suspend,
        Resume,
    }
    let (step, gate) = {
        use crate::network::listener::conn;

        let this = root.as_ref().project_ref();
        let view = this.owner.pool().inspect();
        let Some((slot, pending)) = view.egress(key) else {
            return;
        };
        let gate = A::close_gate(this.app, conn::Ref::new(slot));
        let gated = <A::Input as receive::Policy>::APPLICATION_GATED;
        let pause = gated && A::is_blocked(this.app, conn::Ref::new(slot));
        let close = slot.close_due(gate);
        if slot.is_closing() || (matches!(close, Readiness::Due) && slot.terminal.is_done()) {
            match close {
                Readiness::Due => (Step::Close, Gate::None),
                Readiness::Waiting => (Step::Idle, Gate::None),
            }
        } else {
            let output = flow::SlotFlow::flow(slot, pending);
            let held = <A::Input as receive::Policy>::EGRESS_COUPLED
                && !matches!(output, flow::Flow::Clear)
                && !A::can_receive(this.app, conn::Ref::new(slot));
            let owns = gated || <A::Input as receive::Policy>::EGRESS_COUPLED;
            let gate = if pause || held {
                Gate::Suspend
            } else if owns && !matches!(close, Readiness::Due) {
                Gate::Resume
            } else {
                Gate::None
            };
            let step = match output {
                flow::Flow::Plain | flow::Flow::Deferred if slot.send_status().is_waiting() => {
                    Step::Idle
                }
                flow::Flow::Plain | flow::Flow::Deferred => Step::Retry,
                flow::Flow::Inflight | flow::Flow::Stalled | flow::Flow::Held => Step::Idle,
                flow::Flow::Clear if matches!(close, Readiness::Due) => Step::Close,
                flow::Flow::Clear => Step::Idle,
            };
            (step, gate)
        }
    };
    match gate {
        Gate::None => {}
        Gate::Suspend => root.as_mut().project().owner.pool_mut().suspend(key),
        Gate::Resume => root.as_mut().project().owner.pool_mut().resume(key),
    }
    match step {
        Step::Close => control::close_slot(root.as_mut(), key, turn, driver),
        Step::Retry => {
            let this = root.as_mut().project();
            if let Some(handle) = pending::Pending::of(this.owner.pool()).at(key) {
                handle.wake();
            }
        }
        Step::Idle => {}
    }
}
