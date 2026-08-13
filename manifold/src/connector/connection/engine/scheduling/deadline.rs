use std::{pin, time};

use dope_core::driver::{retained, schedule, schedule::ready::completion};
use dope_net::link::pool;

use crate::{
    connector::{
        app, attempt, auxiliary,
        connection::{self, engine::transition::close},
        lifecycle,
    },
    timing,
};

pub(in crate::connector) trait DeadlinePhase<'d, const ID: u8, A, S, E>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn arm_timeout(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
        now: time::Instant,
        window: timing::Window,
    ) -> bool;

    fn cancel_timeout(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
    );

    fn clear_timeouts(self: pin::Pin<&mut Self>, key: pool::Key<'d, ID>);

    fn refresh_inbound(
        self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn poll_timeouts(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, A, S, E, X> DeadlinePhase<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn arm_timeout(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
        now: time::Instant,
        window: timing::Window,
    ) -> bool {
        let armed =
            self.as_mut()
                .project()
                .schedule
                .deadlines
                .arm_after(key, kind, now, window.get());
        self.as_mut().rearm_deadline();
        armed
    }

    fn cancel_timeout(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        kind: lifecycle::TimeoutKind,
    ) {
        self.as_mut().project().schedule.deadlines.cancel(key, kind);
        self.as_mut().rearm_deadline();
    }

    fn clear_timeouts(mut self: pin::Pin<&mut Self>, key: pool::Key<'d, ID>) {
        self.as_mut().project().schedule.deadlines.clear(key);
        self.as_mut().rearm_deadline();
    }

    fn refresh_inbound(
        mut self: pin::Pin<&mut Self>,
        key: pool::Key<'d, ID>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let state = {
            let this = self.as_ref().project_ref();
            this.pool.get(key).map(|slot| {
                let inbound = this.app.inbound(
                    connection::Ref::new(slot),
                    <E::Timing as timing::Policy>::IDLE_WINDOW,
                    driver,
                );
                (inbound, slot.state.last_recv)
            })
        };
        let Some((inbound, last_recv)) = state else {
            return;
        };
        let valid = match inbound {
            app::Inbound::Quiescent => {
                self.as_mut()
                    .cancel_timeout(key, lifecycle::TimeoutKind::Inbound);
                true
            }
            app::Inbound::Awaiting(window) => {
                let valid = self
                    .as_mut()
                    .project()
                    .schedule
                    .deadlines
                    .arm_inbound(key, now, last_recv, window.get())
                    .is_some();
                self.as_mut().rearm_deadline();
                valid
            }
        };
        if !valid {
            close::ClosePhase::abort_slot(
                self,
                key,
                lifecycle::CloseReason::Timeout(lifecycle::TimeoutKind::Inbound),
                turn,
                driver,
            );
        }
    }

    fn poll_timeouts(
        mut self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        if !self.as_ref().project_ref().wake.deadline_armed() {
            return;
        }
        let now = driver.turn_now();
        let deadline_now = driver.driver_ref().scheduler().deadline(now);
        let ready = self.as_ref().backoff_key();
        let wake = completion::Waker::from_ready(self.as_ref().project_ref().pool.driver(), ready);
        if !self
            .as_mut()
            .project()
            .wake
            .as_mut()
            .poll_deadline(deadline_now, wake)
        {
            return;
        }
        loop {
            if turn.reborrow().maintenance().remaining() == 0 {
                break;
            }
            let expired = self.as_mut().project().schedule.deadlines.pop_expired(now);
            let Some((key, kind)) = expired else {
                break;
            };
            turn.reborrow().maintenance().take();
            if self.as_ref().project_ref().pool.get(key).is_none() {
                self.as_mut().project().schedule.deadlines.clear(key);
                continue;
            }
            if kind == lifecycle::TimeoutKind::Inbound {
                let inbound = {
                    let this = self.as_ref().project_ref();
                    this.pool.get(key).map(|slot| {
                        match this.app.inbound(
                            connection::Ref::new(slot),
                            <E::Timing as timing::Policy>::IDLE_WINDOW,
                            driver,
                        ) {
                            app::Inbound::Quiescent => None,
                            app::Inbound::Awaiting(window) => Some((slot.state.last_recv, window)),
                        }
                    })
                };
                let Some(inbound) = inbound else {
                    continue;
                };
                let Some((last_recv, window)) = inbound else {
                    self.as_mut()
                        .cancel_timeout(key, lifecycle::TimeoutKind::Inbound);
                    continue;
                };
                let deadline = self.as_mut().project().schedule.deadlines.arm_inbound(
                    key,
                    now,
                    last_recv,
                    window.get(),
                );
                if deadline.is_some_and(|deadline| deadline > now) {
                    continue;
                }
            }
            close::ClosePhase::abort_slot(
                self.as_mut(),
                key,
                lifecycle::CloseReason::Timeout(kind),
                turn.reborrow(),
                driver,
            );
        }
        self.as_mut().rearm_deadline();
    }
}

trait RearmDeadline<'d, const ID: u8, A, S, E>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    fn rearm_deadline(self: pin::Pin<&mut Self>);
}

impl<'d, const ID: u8, A, S, E, X> RearmDeadline<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn rearm_deadline(self: pin::Pin<&mut Self>) {
        let ready = self.as_ref().backoff_key();
        let wake = completion::Waker::from_ready(self.as_ref().project_ref().pool.driver(), ready);
        let driver = self.as_ref().project_ref().pool.driver();
        let deadline = self
            .as_ref()
            .project_ref()
            .schedule
            .deadlines
            .earliest()
            .map(|deadline| driver.scheduler().deadline(deadline));
        self.project().wake.as_mut().set_deadline(deadline, wake);
    }
}
