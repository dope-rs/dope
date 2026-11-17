use std::pin;

use dope_core::driver::{self, route::Identity, schedule};
use dope_net::link::{
    pool,
    slot::types::{CloseGate, Readiness},
};

use crate::{
    app::env,
    client::{
        app, attempt,
        close::{self, Directive},
        conn::{
            self, State, claim, closing, drain,
            engine::{self, liveness},
            flow, state,
        },
        side::mode::Mode,
    },
    timing,
};

mod finalize;

pub(in crate::client) trait ClosePhase<'p, 'w, 'd: 'w, ID: Identity, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    /// Discarded work stays with the close callback instead of re-entering the send queue.
    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn cut_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        rule: Directive,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn maybe_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );
    fn close_entry<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>>;

    fn maybe_entry<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>>;

}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> ClosePhase<'p, 'w, 'd, ID, A, S, E, X>
    for engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    S::Mode: finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>,
{
    fn cut_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        rule: Directive,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (pool, mut context) = self.parts();
        let Ok(mut entry) = pool.entry(key) else { return };
        match State::apply(entry.get_mut(), rule) {
            flow::Apply::Applied(flow::Flow::Continue(()))
            | flow::Apply::Applied(flow::Flow::Discarded(())) => {}
            flow::Apply::Sealed => return,
        }
        drop(Self::close_entry(&mut context, entry, rule.reason(), turn, driver));
    }

    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (pool, mut context) = self.parts();
        if let Ok(entry) = pool.entry(key) {
            drop(Self::close_entry(&mut context, entry, reason, turn, driver));
        }
    }

    fn close_entry<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        mut entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>> {
        let rule = {
            let Some(view) = entry.egress_mut() else { return Some(entry) };
            let slot = view.connection;
            let key = slot.key();
            let _ = State::poll_close(slot, context.app);
            if reason == close::Reason::EndpointRetired
                && slot.state.closing.is_withdrawing()
                && let Some(conn) = slot.state.conn_mut()
                && matches!(context.app.withdraw(conn::Id::from_key(key), conn), app::Fence::Pending)
            {
                return Some(entry);
            }
            let pending = slot.terminal.is_pending();
            let wake = if pending {
                slot.terminal.has_send()
            } else {
                !slot.is_discarded() && slot.state.work.has_next()
            };
            let owner = slot.state.owner.attempt();
            let closing = slot.state.closing.apply(Directive::reconnect(reason));
            <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::begin(
                context.controller, context.app, owner, closing.effective().reason(), driver,
            );
            if pending || wake {
                if wake { view.pending.wake(); }
                return Some(entry);
            }
            let rule = closing.effective();
            match closing.claim_final() {
                closing::ClaimState::Issued(claim) => {
                    let close = drain::Drain::new(conn::Id::from_key(key), claim);
                    let close = drain::OwnedDrain::new(owner, close);
                    return <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(
                        context, entry, close, turn, driver,
                    );
                }
                closing::ClaimState::Active => return Some(entry),
                closing::ClaimState::InProgress => {}
            }
            rule
        };
        let close = {
            let Some(view) = entry.egress_mut() else { return Some(entry) };
            let key = view.connection.key();
            // SAFETY: this context and entry retain the original primary pool and book.
            let source = unsafe { engine::raw::reunite(context.liveness, view.connection, key) };
            let mut slot = liveness::Change::new(source, context.target);
            view.queue.cancel_wait(driver.region_token());
            match State::close(&mut slot, rule, |active| {
                context.app.close(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    view.queue.reborrow(), rule.reason(), driver,
                )
            }) {
                flow::Close::Complete(close, urgency) => {
                    let slot = slot.closing(
                        driver.driver_ref().clock().now(),
                        <E::Timing as timing::Policy>::SEND_DEADLINE,
                    );
                    if matches!(urgency, claim::Urgency::Immediate) { slot.discard(); }
                    drain::OwnedDrain::new(slot.state.owner.attempt(), close)
                }
                flow::Close::Yield => {
                    view.pending.wake();
                    return Some(entry);
                }
                flow::Close::Blocked | flow::Close::Sealed => return Some(entry),
            }
        };
        <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(
            context, entry, close, turn, driver,
        )
    }

    fn maybe_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let (pool, mut context) = self.parts();
        if let Ok(entry) = pool.entry(key) {
            drop(Self::maybe_entry(&mut context, entry, turn, driver));
        }
    }

    fn maybe_entry<'a>(
        context: &mut engine::Context<'_, 'd, ID, A, S, E, X>,
        mut entry: engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<engine::Entry<'a, 'p, 'w, 'd, ID, A, E::Transport>> {
        match ready(context.app, entry.get_mut(), driver) {
            Some(reason) => Self::close_entry(context, entry, reason, turn, driver),
            None => Some(entry),
        }
    }
}

pub(in crate::client::conn::engine) fn ready<'w, 'd: 'w, ID: Identity, A>(
    app: &mut A,
    slot: &mut engine::Slot<'w, 'd, ID, A>,
    driver: &mut driver::Context<'_, 'd>,
) -> Option<close::Reason>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'w, 'd, ID>,
{
    if !slot.state.closing.is_withdrawing() {
        if matches!(slot.close_due(CloseGate::Immediate), Readiness::Waiting) {
            return None;
        }
        if slot.is_draining()
            && let Some(active) = state::ActiveRef::new(slot)
            && matches!(
                app::Receive::fence(app, conn::Ref::new(active), driver),
                app::Fence::Pending
            )
        {
            return None;
        }
    }
    slot.state.closing.reason()
}
