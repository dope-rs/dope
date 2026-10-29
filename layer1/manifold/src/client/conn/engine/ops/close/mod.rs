use std::pin;

use attempt::Report as _;
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
            self, State, claim, drain,
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
        reason: close::Reason,
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
    S::Error: attempt::Report<'d, ID, A>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
    S::Mode: finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>,
{
    fn cut_close(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        {
            let this = self.as_mut().project();
            let Some(slot) = this.links.pool_mut().get_mut(key) else {
                return;
            };
            match State::apply(slot, close::Directive::cut_retry(reason)) {
                flow::Apply::Applied(flow::Flow::Continue(()))
                | flow::Apply::Applied(flow::Flow::Discarded(())) => {}
                flow::Apply::Sealed => return,
            }
        }
        self.close_slot(key, reason, turn, driver);
    }

    fn close_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: close::Reason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let deferred = {
            let this = self.as_mut().project();
            pending::Mut::of(this.links.pool_mut())
                .get(key)
                .is_some_and(|(slot, handle)| {
                    slot.state.closing.apply(Directive::reconnect(reason));
                    if slot.terminal.is_pending() {
                        if slot.terminal.has_send() {
                            handle.wake();
                        }
                        return true;
                    }
                    if !slot.is_discarded() && slot.state.work.has_next() {
                        handle.wake();
                        return true;
                    }
                    false
                })
        };
        if deferred {
            return;
        }
        {
            let this = self.as_mut().project();
            let Some(slot) = this.links.pool_mut().get_mut(key) else {
                return;
            };
            match claim::ProbeClaim::claim(slot) {
                claim::ProbeClaim::Ready(close) => {
                    let close =
                        drain::OwnedDrain::new(slot.state.owner.attempt(), close.into_drain());
                    <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(
                        finalize::Finalizing {
                            machine: self,
                            close,
                            turn,
                            driver,
                        },
                    );
                    return;
                }
                claim::ProbeClaim::Active => return,
                claim::ProbeClaim::InProgress | claim::ProbeClaim::Missing => {}
            }
        }
        let target = self.as_ref().backoff();
        let outcome = {
            let this = self.as_mut().project();
            let Some(liveness::Egress {
                flights: _,
                connection: mut slot,
                pending: _,
                queue: egress,
            }) = this.links.egress(key, target)
            else {
                return;
            };
            egress.cancel_wait(driver.region_token());
            let rule = slot.state.reconnect(reason);
            let outcome = match State::close(&mut slot, rule, |active| {
                this.app.close(
                    conn::Ctx::new(active, turn.reborrow().app()),
                    egress.reborrow(),
                    rule.reason(),
                    driver,
                )
            }) {
                flow::Close::Ready(
                    flow::Flow::Continue(output) | flow::Flow::Discarded(output),
                ) => output,
                flow::Close::Sealed => return,
            };
            if matches!(outcome, app::CloseOutcome::Complete(_)) {
                slot.closing(
                    driver.driver_ref().clock().now(),
                    <E::Timing as timing::Policy>::SEND_DEADLINE,
                );
            }
            outcome
        };
        match outcome {
            app::CloseOutcome::Complete(_) => {}
            app::CloseOutcome::Yield => {
                let this = self.project();
                if let Some((_, handle)) = pending::Mut::of(this.links.pool_mut()).get(key) {
                    handle.wake();
                }
                return;
            }
        }
        let close = {
            let this = self.as_mut().project();
            let Some(close) =
                claim::Settlement::claim(this.links.pool_mut().get_mut(key)).drain(|owner| {
                    if let Err(cause) = this.controller.kill(owner, None) {
                        cause.retire(
                            attempt::Completion::reported(owner, this.controller),
                            this.app,
                            driver,
                        );
                    }
                    if this.shutdown.is_open() && this.controller.is_pending() {
                        this.drive.target().wake();
                    }
                })
            else {
                return;
            };
            close
        };
        <S::Mode as finalize::Finalize<'p, 'w, 'd, ID, A, S, E, X>>::finish(finalize::Finalizing {
            machine: self,
            close,
            turn,
            driver,
        });
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
