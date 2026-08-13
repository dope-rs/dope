use std::pin;

use dope_core::driver::{retained, schedule};
use dope_net::link::pool::{self, pending};

use crate::connector::{
    app, attempt, auxiliary,
    auxiliary::{Kind as _, Ownership as _},
    connection::{
        self,
        engine::{scheduling::deadline, transition::retire},
    },
    lifecycle,
};

fn apply_close_kind(
    closing: &mut connection::Closing,
    kind: Option<app::CloseKind>,
    reason: lifecycle::CloseReason,
) {
    match kind {
        Some(app::CloseKind::Reconnect) => {
            closing.request(reason);
        }
        Some(app::CloseKind::Permanent) => {
            closing.request(reason);
            closing.request_permanent();
        }
        None => {}
    }
}

pub(in crate::connector) trait ClosePhase<'d, const ID: u8, A, S, E, O>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn close_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: lifecycle::CloseReason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn abort_slot(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: lifecycle::CloseReason,
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

impl<'d, const ID: u8, A, S, E, X> ClosePhase<'d, ID, A, S, E, X::Owner>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn abort_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: lifecycle::CloseReason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if let Some(slot) = self.as_mut().project().pool.get_mut(key) {
            slot.abort();
            slot.state.request_close(reason);
        }
        self.close_slot(key, reason, turn, driver);
    }

    fn close_slot(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        reason: lifecycle::CloseReason,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let deferred = {
            let this = self.as_mut().project();
            pending::Mut::of(this.pool)
                .get(key)
                .is_some_and(|(slot, handle)| {
                    if !handle.contains(pending::Action::Ingress) {
                        return false;
                    }
                    slot.state.request_close(reason);
                    handle.mark(pending::Action::Close);
                    true
                })
        };
        if deferred {
            return;
        }
        let draining = self
            .as_ref()
            .project_ref()
            .pool
            .get(key)
            .is_some_and(|slot| slot.state.closing.is_draining());
        if draining {
            let this = self.project();
            <Self as retire::RetirePhase<'d, ID, A, S, E, X::Owner>>::drain_close(
                this.pool,
                this.app,
                key,
                turn.reborrow(),
                driver,
            );
            return;
        }
        let now = driver.turn_now();
        let outcome = {
            let this = self.as_mut().project();
            let Some(pool::EgressMut {
                flights: _,
                connection: slot,
                queue: egress,
            }) = this.pool.egress_mut(key)
            else {
                return;
            };
            let reason = slot.state.request_close(reason);
            if this.auxiliary.settle(
                &mut slot.state.owner,
                Err(if matches!(reason, lifecycle::CloseReason::Timeout(_)) {
                    auxiliary::Error::Timeout
                } else {
                    auxiliary::Error::Transport
                }),
                driver.region_token(),
            ) {
                slot.state.closing.request_permanent();
                app::CloseOutcome::Complete(reason)
            } else if slot.state.owner.is_auxiliary() {
                app::CloseOutcome::Complete(reason)
            } else if slot.is_established() {
                this.app.close(
                    connection::Ctx::new(slot, turn.reborrow().application()),
                    egress,
                    reason,
                    driver,
                )
            } else {
                app::CloseOutcome::Complete(reason)
            }
        };
        let reason = match outcome {
            app::CloseOutcome::Complete(reason) => reason,
            app::CloseOutcome::Yield => {
                let this = self.project();
                if let Some((slot, handle)) = pending::Pending::of(this.pool).get(key) {
                    handle.mark(pending::Action::Close);
                    driver
                        .driver_ref()
                        .ready()
                        .activate_ready(slot.io().ready_key());
                }
                return;
            }
        };
        let close_kind = {
            let this = self.as_ref().project_ref();
            this.pool.get(key).and_then(|slot| {
                slot.state
                    .owner
                    .attempt()
                    .and_then(|_| this.app.close_kind(connection::Ref::new(slot), driver))
            })
        };
        deadline::DeadlinePhase::clear_timeouts(self.as_mut(), key);
        let this = self.project();
        let retirement = this.pool.get_mut(key).and_then(|slot| {
            apply_close_kind(&mut slot.state.closing, close_kind, reason);
            slot.state
                .closing
                .retire(reason)
                .map(|retirement| (slot.state.owner.attempt(), retirement))
        });
        match retirement {
            Some((Some(key), connection::Retirement::Reconnect(reason))) => {
                this.controller.disconnect(key, reason, now);
            }
            Some((Some(key), connection::Retirement::Permanent)) => this.controller.kill(key),
            Some((None, _)) | None => {}
        }
        <Self as retire::RetirePhase<'d, ID, A, S, E, X::Owner>>::drain_close(
            this.pool, this.app, key, turn, driver,
        );
    }

    fn maybe_close(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let close = {
            let this = self.as_mut().project();
            let Some(slot) = this.pool.get_mut(key) else {
                return;
            };
            if slot.state.owner.is_auxiliary() {
                slot.should_close(false)
            } else {
                let close_kind = this.app.close_kind(connection::Ref::new(&*slot), driver);
                apply_close_kind(
                    &mut slot.state.closing,
                    close_kind,
                    lifecycle::CloseReason::Local,
                );
                slot.should_close(this.app.defer_close(connection::Ref::new(slot), driver))
            }
        };
        if close {
            self.as_mut()
                .close_slot(key, lifecycle::CloseReason::Local, turn, driver);
        }
    }
}
