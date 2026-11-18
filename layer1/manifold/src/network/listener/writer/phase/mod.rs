use std::pin;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::link::{
    pool::{self, pending},
    slot::{
        transmit::outcome::Retention,
        types::{self, Readiness},
    },
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
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Runtime<'w, 'd, ID>,
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
            let this = root.as_ref().project_ref();
            let view = this.owner.pool().inspect();
            let Some((slot, pending)) = view.egress(key) else {
                return pending::Disposition::Gone;
            };
            let sending = slot.send_status();
            let waiting = sending.is_waiting();
            let ready = capacity
                || (waiting && sending.retention() == Retention::Held)
                || match flow::flow(slot, &slot.state.send, pending) {
                    flow::Flow::Done => true,
                    flow::Flow::Plain | flow::Flow::Deferred => !waiting,
                    _ => false,
                };
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
                advance(root.as_mut(), key, wake, turn.reborrow(), driver);
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
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let mut this = root.as_mut().project();
    let writer::Owner {
        pool,
        deadlines,
        writes,
    } = this.owner;
    let Ok(mut entry) = pool.entry(key) else {
        return;
    };
    let Some(source) = entry.sender() else { return };
    sender::commit_end::<ID, A, _>(source, deadlines, this.app.as_mut(), driver);
    if finish(&mut entry, this.app.as_ref()) {
        drop(writer::close(
            Ok(entry), *writes, &mut (this.app, this.accept, deadlines), turn, driver,
        ));
    }
}

pub(in crate::network::listener) fn advance<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    wake: Wake,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let this = root.as_mut().project();
    let writer::Owner {
        pool,
        deadlines,
        writes,
    } = this.owner;
    let Ok(entry) = pool.entry(key) else {
        return;
    };
    drop(advance_entry(
        entry,
        *writes,
        &mut (this.app, this.accept, deadlines),
        wake,
        turn,
        driver,
    ));
}

pub(in crate::network::listener) fn advance_entry<
    'a, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport,
>(
    mut entry: writer::Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A>,
    writes: &'p writer::memory::Arena<'s, 'd, ID, A::Egress>,
    ctx: &mut writer::Closing<'_, '_, '_, 'p, 'd, ID, A>,
    wake: Wake,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<writer::Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A>>
where
    A: handler::Runtime<'w, 'd, ID>,
{
    let Some(source) = entry.sender() else { return Some(entry) };
    let (app, _, deadlines) = &mut *ctx;
    if matches!(
        sender::commit_chunk::<ID, A, _>(
            source, deadlines, app.as_mut(), wake, driver,
        ),
        flow::Flow::Done
    ) {
        send::complete(
            source, deadlines, writes, app.as_mut(), turn.reborrow(), driver,
        );
    }
    if finish(&mut entry, ctx.0.as_ref()) {
        writer::close(Ok(entry), writes, ctx, turn, driver)
    } else {
        Some(entry)
    }
}

pub(in crate::network::listener) fn finish<'a, 'n, 'p, 's, 'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
    entry: &mut writer::Entry<'a, 'n, 'p, 's, 'w, 'd, ID, T, A>,
    app: pin::Pin<&A>,
) -> bool
where
    A: handler::Application<'d, ID>,
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
        let slot = entry.get();
        let pending = entry.egress();
        let gate = A::close_gate(app.as_ref(), conn::Ref::new(slot));
        let owns = <A::Input as receive::Policy>::APPLICATION_GATED
            || <A::Input as receive::Policy>::EGRESS_COUPLED;
        let enabled = owns && A::is_gated(&slot.state.conn);
        let gated = enabled && <A::Input as receive::Policy>::APPLICATION_GATED;
        let pause = gated && A::is_blocked(app.as_ref(), conn::Ref::new(slot));
        let close = slot.close_due(gate);
        if slot.is_closing() || (matches!(close, Readiness::Due) && slot.terminal.is_done()) {
            match close {
                Readiness::Due => (Step::Close, Gate::None),
                Readiness::Waiting => (Step::Idle, Gate::None),
            }
        } else {
            let output = flow::flow(slot, &slot.state.send, pending);
            let held = enabled
                && <A::Input as receive::Policy>::EGRESS_COUPLED
                && !matches!(output, flow::Flow::Clear)
                && !A::can_receive(app.as_ref(), conn::Ref::new(slot));
            let gate = if pause || held {
                Gate::Suspend
            } else if owns
                && (enabled || slot.is_app_held() || slot.terminal.is_read_done())
                && !matches!(close, Readiness::Due)
            {
                Gate::Resume
            } else {
                Gate::None
            };
            let step = match output {
                flow::Flow::Plain | flow::Flow::Deferred if slot.send_status().is_waiting() => {
                    Step::Idle
                }
                flow::Flow::Done | flow::Flow::Plain | flow::Flow::Deferred => Step::Retry,
                flow::Flow::Inflight | flow::Flow::Stalled | flow::Flow::Held => Step::Idle,
                flow::Flow::Clear if matches!(close, Readiness::Due) => Step::Close,
                flow::Flow::Clear => Step::Idle,
            };
            (step, gate)
        }
    };
    match gate {
        Gate::None => {}
        Gate::Suspend => entry.suspend(),
        Gate::Resume => entry.resume(),
    }
    match step {
        Step::Close => true,
        Step::Retry => {
            entry.pending().wake();
            false
        }
        Step::Idle => false,
    }
}

pub(in crate::network::listener) fn close_ready<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    key: pool::Key<'d, ID>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Application<'d, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let this = root.as_mut().project();
    let writer::Owner {
        pool,
        deadlines,
        writes,
    } = this.owner;
    if let Ok(mut entry) = pool.entry(key)
        && finish(&mut entry, this.app.as_ref())
    {
        drop(writer::close(
            Ok(entry), *writes, &mut (this.app, this.accept, deadlines), turn, driver,
        ));
    }
}
