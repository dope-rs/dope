use std::pin;

use dope_core::driver::{retained, route::Identity, schedule};
use dope_net::link::{
    pool::{self, pending},
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

pub(in crate::client) trait ClosePhase<'d, ID: Identity, A, S, E>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    /// Discarded work stays with the close callback instead of re-entering the send queue.
    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn cut_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        rule: Directive,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn maybe_close(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X> ClosePhase<'d, ID, A, S, E>
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
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        rule: Directive,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        {
            let this = self.as_mut().project();
            let Some(slot) = this.links.pool_mut().get_mut(key) else {
                return;
            };
            match State::apply(slot, rule) {
                flow::Apply::Applied(flow::Flow::Continue(()))
                | flow::Apply::Applied(flow::Flow::Discarded(())) => {}
                flow::Apply::Sealed => return,
            }
        }
        self.close_slot(key, rule.reason(), turn, driver);
    }

    fn close_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let rule = {
            let this = self.as_mut().project();
            let Some((slot, handle)) = pending::Mut::of(this.links.pool_mut()).get(key) else {
                return;
            };
            let pending = slot.terminal.is_pending();
            let wake = if pending {
                slot.terminal.has_send()
            } else {
                !slot.is_discarded() && slot.state.work.has_next()
            };
            let closing = slot.state.closing.apply(Directive::reconnect(reason));
            if pending || wake {
                if wake {
                    handle.wake();
                }
                return;
            }
            let rule = closing.effective();
            match closing.claim_final() {
                closing::ClaimState::Issued(claim) => {
                    let close = drain::Drain::new(conn::Id::from_key(slot.key()), claim);
                    let close = drain::OwnedDrain::new(slot.state.owner.attempt(), close);
                    <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(
                        self, close, turn, driver,
                    );
                    return;
                }
                closing::ClaimState::Active => return,
                closing::ClaimState::InProgress => {}
            }
            rule
        };
        let target = self.as_ref().backoff();
        let close = {
            let this = self.as_mut().project();
            let Some(liveness::Egress {
                flights: _,
                connection: mut slot,
                pending: handle,
                queue: egress,
            }) = this.links.egress(key, target)
            else {
                return;
            };
            egress.cancel_wait(driver.region_token());
            match State::close(&mut slot, rule, |active| {
                this.app.close(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    rule.reason(),
                    driver,
                )
            }) {
                flow::Close::Complete(close, urgency) => {
                    let slot = slot.closing(
                        driver.driver_ref().clock().now(),
                        <E::Timing as timing::Policy>::SEND_DEADLINE,
                    );
                    if matches!(urgency, claim::Urgency::Immediate) {
                        slot.discard();
                    }
                    drain::OwnedDrain::new(slot.state.owner.attempt(), close)
                }
                flow::Close::Yield => {
                    handle.wake();
                    return;
                }
                flow::Close::Blocked | flow::Close::Sealed => return,
            }
        };
        <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(
            self, close, turn, driver,
        );
    }

    fn maybe_close(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let reason = {
            let this = self.as_mut().project();
            let Some(slot) = this.links.pool_mut().get_mut(key) else {
                return;
            };
            let fence = match state::ActiveRef::new(slot) {
                Some(active) => app::Receive::fence(this.app, conn::Ref::new(active), driver),
                None => app::Fence::Ready,
            };
            let ready = match fence {
                app::Fence::Ready => slot.close_due(CloseGate::Immediate),
                app::Fence::Pending => slot.close_due(CloseGate::Deferred),
            };
            if matches!(ready, Readiness::Waiting) {
                return;
            }
            slot.state.closing.reason()
        };
        if let Some(reason) = reason {
            self.close_slot(key, reason, turn, driver);
        }
    }
}
