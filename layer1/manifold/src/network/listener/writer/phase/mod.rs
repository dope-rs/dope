use std::{ops::ControlFlow, pin};

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::link::{
    egress,
    pool::{self, pending},
    slot::{transmit::outcome::Retention, types::{self, Readiness}},
};

use crate::{
    app::{env, receive},
    network::listener::{
        self, conn, handler,
        writer::{self, flow},
    },
};

pub(in crate::network::listener) mod send;
mod sender;

#[derive(Clone, Copy)]
pub(in crate::network::listener) enum Wake {
    Maintenance(flow::Flow),
    Io,
    Progress,
}

pub(in crate::network::listener) fn flush_dirty<'w, 'd: 'w, ID: Identity, A, E>(
    mut root: pin::Pin<&mut listener::Listener<'_, '_, '_, 'w, 'd, ID, A, E>>,
    turn: schedule::Turn<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
) where
    A: handler::Runtime<'w, 'd, ID>,
    E: env::Spec<Wire = A::Wire>,
{
    let Some(round) = pending::Pending::of(root.as_ref().project_ref().owner.pool()).ready_round()
    else {
        return;
    };
    round.run(turn.reborrow().maint(), |key| {
        let this = root.as_mut().project();
        let writer::Owner { pool, deadlines, writes } = this.owner;
        let Ok(resident) = pool.entry(key) else {
            return pending::Disposition::Gone;
        };
        let mut app = this.app;
        let mut active;
        let action = 'apply: {
            let slot = resident.get();
            let action = 'observe: {
                match slot.demand() {
                    types::Demand::Discard | types::Demand::Close => break 'observe Some(Action::Close),
                    types::Demand::Barrier => {
                        break 'observe current::<ID, A>(slot, resident.egress(), app.as_ref());
                    }
                    types::Demand::Terminal => {
                        active = resident;
                        let Some(source) = active.sender() else { return pending::Disposition::Idle };
                        sender::commit_end::<ID, A, _>(source, deadlines, app.as_mut(), driver);
                    }
                    types::Demand::Continue => {
                        let unheld = flow::unheld(slot, &slot.state.send, resident.egress());
                        let flow = unheld.observe(slot.send_status());
                        let flow = if resident.pending().has_capacity() {
                            flow.retry()
                        } else {
                            let sending = slot.send_status();
                            let waiting = sending.is_waiting();
                            if !(waiting && sending.retention() == Retention::Held) && !flow.runnable(waiting) {
                                break 'observe match Open::read::<ID, A>(slot, app.as_ref()) {
                                    ControlFlow::Break(action) => action,
                                    ControlFlow::Continue(open) => open.finish::<ID, A>(slot, app.as_ref(), unheld),
                                };
                            }
                            flow
                        };
                        active = resident;
                        let Some(source) = active.sender() else { return pending::Disposition::Idle };
                        sender::advance::<ID, A, _>(
                            source, deadlines, *writes, app.as_mut(), Wake::Maintenance(flow),
                            turn.reborrow(), driver,
                        );
                    }
                }
                let Some(action) = current::<ID, A>(active.get(), active.egress(), app.as_ref()) else {
                    return pending::Disposition::Idle;
                };
                break 'apply action;
            };
            let Some(action) = action else { return pending::Disposition::Idle };
            active = resident;
            action
        };
        if action.apply::<ID, A, _>(&mut active)
            && writer::close(
                Ok(active), *writes, &mut (app, this.accept, deadlines), turn.reborrow(), driver,
            ).is_none()
        {
            pending::Disposition::Gone
        } else {
            pending::Disposition::Idle
        }
    });
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
    sender::advance::<ID, A, _>(
        source, deadlines, writes, app.as_mut(), wake, turn.reborrow(), driver,
    );
    if current::<ID, A>(entry.get(), entry.egress(), ctx.0.as_ref())
        .is_some_and(|action| action.apply::<ID, A, _>(&mut entry))
    {
        writer::close(Ok(entry), writes, ctx, turn, driver)
    } else {
        Some(entry)
    }
}

type Connection<'p, 's, 'w, 'd, ID, A> = types::Connection<
    'w, 'd, ID, <A as handler::Application<'d, ID>>::Wire, writer::Record<'p, 's, 'd, ID, A>,
>;

pub(in crate::network::listener) enum Action {
    Close,
    Retry,
    Suspend(Step),
    Resume(Step),
}

pub(in crate::network::listener) enum Step {
    Close,
    Retry,
    Idle,
}

impl Action {
    pub(in crate::network::listener) fn apply<'w, 'd: 'w, ID: Identity, A, T: dope_net::Transport>(
        self,
        entry: &mut writer::Entry<'_, '_, '_, '_, 'w, 'd, ID, T, A>,
    ) -> bool
    where
        A: handler::Application<'d, ID>,
    {
        let step = match self {
            Self::Close => return true,
            Self::Retry => Step::Retry,
            Self::Suspend(step) => { entry.suspend(); step }
            Self::Resume(step) => { entry.resume(); step }
        };
        match step {
            Step::Close => true,
            Step::Retry => { entry.pending().wake(); false }
            Step::Idle => false,
        }
    }
}

struct Open {
    close: Readiness,
    enabled: bool,
    paused: bool,
}

impl Open {
    fn read<'w, 'd: 'w, ID: Identity, A: handler::Application<'d, ID>>(
        slot: &Connection<'_, '_, 'w, 'd, ID, A>, app: pin::Pin<&A>,
    ) -> ControlFlow<Option<Action>, Self> {
        let gate = A::close_gate(app.as_ref(), conn::Ref::new(slot));
        let enabled = (<A::Input as receive::Policy>::APPLICATION_GATED
            || <A::Input as receive::Policy>::EGRESS_COUPLED) && A::is_gated(&slot.state.conn);
        let paused = enabled && <A::Input as receive::Policy>::APPLICATION_GATED
            && A::is_blocked(app, conn::Ref::new(slot));
        let close = slot.close_due(gate);
        if slot.is_closing() || (close == Readiness::Due && slot.terminal.is_done()) {
            return ControlFlow::Break((close == Readiness::Due).then_some(Action::Close));
        }
        ControlFlow::Continue(Self { close, enabled, paused })
    }

    fn finish<'w, 'd: 'w, ID: Identity, A: handler::Application<'d, ID>>(
        self, slot: &Connection<'_, '_, 'w, 'd, ID, A>, app: pin::Pin<&A>, unheld: flow::Flow,
    ) -> Option<Action> {
        let output = unheld.observe(slot.send_status());
        let held = self.enabled && <A::Input as receive::Policy>::EGRESS_COUPLED
            && !matches!(output, flow::Flow::Clear) && !A::can_receive(app, conn::Ref::new(slot));
        let step = if output.runnable(slot.send_status().is_waiting()) {
            Step::Retry
        } else if matches!(output, flow::Flow::Clear) && self.close == Readiness::Due {
            Step::Close
        } else {
            Step::Idle
        };
        if self.paused || held {
            return Some(Action::Suspend(step));
        }
        if (<A::Input as receive::Policy>::APPLICATION_GATED || <A::Input as receive::Policy>::EGRESS_COUPLED)
            && self.close == Readiness::Waiting && (slot.is_app_held() || slot.terminal.is_read_done())
        {
            return Some(Action::Resume(step));
        }
        match step {
            Step::Close => Some(Action::Close),
            Step::Retry => Some(Action::Retry),
            Step::Idle => None,
        }
    }
}

pub(in crate::network::listener) fn current<'w, 'd: 'w, ID: Identity, A: handler::Application<'d, ID>>(
    slot: &Connection<'_, '_, 'w, 'd, ID, A>, pending: egress::Pending, app: pin::Pin<&A>,
) -> Option<Action> {
    match Open::read::<ID, A>(slot, app) {
        ControlFlow::Break(action) => action,
        ControlFlow::Continue(open) => open.finish::<ID, A>(slot, app, flow::unheld(slot, &slot.state.send, pending)),
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
        && current::<ID, A>(entry.get(), entry.egress(), this.app.as_ref())
            .is_some_and(|action| action.apply::<ID, A, _>(&mut entry))
    {
        drop(writer::close(
            Ok(entry), *writes, &mut (this.app, this.accept, deadlines), turn, driver,
        ));
    }
}
