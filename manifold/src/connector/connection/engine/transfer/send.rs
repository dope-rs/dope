use std::pin;

use dope_core::driver::{self, retained, route, schedule};
use dope_net::{
    link::{
        pool::{self, pending},
        slot::send,
    },
    wire::{self, reclaim},
};

use crate::{
    connector::{
        app, attempt,
        auxiliary::{self, Kind as _},
        connection::{
            self,
            engine::{self, scheduling::deadline, transition::close},
        },
        lifecycle,
    },
    timing,
};

pub(in crate::connector::connection::engine) trait SendPhase<'d, const ID: u8, A, S, E, O>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn drain_requests(
        app: &A,
        pool: &mut engine::Pool<'d, ID, A, E, O>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        use crate::connector::app::CloseKind;

        let Some(pending::ScheduledEgress {
            flights: _,
            connection: slot,
            pending: handle,
            queue,
        }) = pending::Mut::of(pool).egress(key)
        else {
            return;
        };
        let mut drain = app::RequestDrain::new(queue, turn.application());
        let requests = app.drain_requests(
            connection::Id::from_key(key),
            &mut slot.state.conn,
            &mut drain,
            driver,
        );
        if drain.enqueued() {
            handle.mark(pending::Action::Egress);
        }
        if drain.exhausted() {
            driver
                .driver_ref()
                .ready()
                .activate_ready(slot.io().ready_key());
        }
        match requests.close {
            Some(CloseKind::Reconnect) => handle.mark(pending::Action::Close),
            Some(CloseKind::Permanent) => {
                slot.state.closing.request_permanent();
                handle.mark(pending::Action::Close);
            }
            None => {}
        }
    }

    fn apply_requests(
        self: pin::Pin<&mut Self>,
        target: route::Token,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn submit_egress(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> SendPhase<'d, ID, A, S, E, X::Owner>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Receive<'d, ID> + app::Lifecycle<'d, ID> + app::RequestSource<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn apply_requests(
        mut self: pin::Pin<&mut Self>,
        target: route::Token,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.as_mut().project();
        let Some((key, _)) = this.pool.by_target(target) else {
            return;
        };
        if this
            .pool
            .get(key)
            .is_some_and(|slot| slot.state.owner.is_auxiliary())
        {
            return;
        }
        Self::drain_requests(this.app, this.pool, key, turn, driver);
    }

    fn submit_egress(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (arm_send, refresh_inbound, drain_on_submit) = {
            let this = self.as_mut().project();
            let Some(pending::ScheduledEgress {
                flights,
                connection: slot,
                pending: handle,
                queue: mut egress,
            }) = this.pool.send_slot(key)
            else {
                return;
            };
            if !slot.is_established() {
                return;
            }
            let auxiliary = slot.state.owner.is_auxiliary();
            if !auxiliary {
                this.app.before_send(
                    connection::Ctx::new(slot, turn.reborrow().application()),
                    egress.reborrow(),
                    driver,
                );
            }
            let was_inflight = slot.send_status().inflight();
            let progress = slot.sending().submit_egress(&mut egress, flights, driver);
            match progress {
                Ok(send::Progress::Runnable) => {
                    handle.mark(pending::Action::Egress);
                }
                Ok(send::Progress::Waiting | send::Progress::Quiescent) => {}
                Err(_) => {
                    slot.abort();
                    handle.mark(pending::Action::Close);
                }
            }
            (
                !auxiliary && !was_inflight && slot.send_status().inflight(),
                !auxiliary,
                !auxiliary && <<A::Wire as wire::Wire>::Reclaim as reclaim::Policy>::ON_SUBMIT,
            )
        };
        if drain_on_submit {
            let this = self.as_mut().project();
            Self::drain_requests(this.app, this.pool, key, turn.reborrow(), driver);
        }
        if arm_send
            && !deadline::DeadlinePhase::arm_timeout(
                self.as_mut(),
                key,
                lifecycle::TimeoutKind::Send,
                driver.turn_now(),
                <E::Timing as timing::Policy>::SEND_DEADLINE,
            )
        {
            return close::ClosePhase::abort_slot(
                self.as_mut(),
                key,
                lifecycle::CloseReason::Timeout(lifecycle::TimeoutKind::Send),
                turn,
                driver,
            );
        }
        if refresh_inbound {
            deadline::DeadlinePhase::refresh_inbound(
                self.as_mut(),
                key,
                driver.turn_now(),
                turn.reborrow(),
                driver,
            );
        }
        close::ClosePhase::maybe_close(self, key, turn, driver);
    }
}
