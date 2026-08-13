use std::{pin, time};

use dope_core::driver::{self, retained, schedule, schedule::ready::completion};
use dope_net::link::pool::transition::open;

use crate::{
    connector::{
        app, attempt, auxiliary,
        connection::{
            self,
            engine::{scheduling::deadline, transition::close},
        },
        lifecycle,
    },
    timing,
};

const SUBMISSION_RETRY_BASE: time::Duration = time::Duration::from_millis(1);
const MAX_SUBMISSION_RETRY_SHIFT: u8 = 10;

pub(in crate::connector) trait DialPhase<'d, const ID: u8, A, S, E>
where
    A: app::Lifecycle<'d, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
{
    /// Returns true when this turn armed the bounded submission retry timer.
    fn poll_source(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool;

    fn arm_backoff(self: pin::Pin<&mut Self>, deadline: time::Instant);

    fn submission_retry_ready(
        self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool;

    fn defer_submission(
        self: pin::Pin<&mut Self>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );

    fn submission_succeeded(self: pin::Pin<&mut Self>);
}

impl<'d, const ID: u8, A, S, E, X> DialPhase<'d, ID, A, S, E>
    for connection::Engine<'d, ID, A, S, E, X>
where
    A: app::Lifecycle<'d, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: crate::Env<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: auxiliary::Mode<'d, A::Send, ID>,
{
    fn poll_source(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        let mut this = self;
        if this.as_ref().project_ref().wake.is_draining() {
            return false;
        }
        let now = driver.turn_now();
        let deadline_now = driver.driver_ref().scheduler().deadline(now);
        let backoff_fired = {
            let ready = this.as_ref().backoff_key();
            let wake =
                completion::Waker::from_ready(this.as_ref().project_ref().pool.driver(), ready);
            this.as_mut()
                .project()
                .wake
                .as_mut()
                .poll_backoff(deadline_now, wake)
        };
        if !this.as_ref().project_ref().controller.has_pending() && !backoff_fired {
            return false;
        }
        let cap = *this.as_ref().project_ref().primary_capacity;
        for _ in 0..cap {
            if !turn.reborrow().maintenance().take() {
                return true;
            }
            use crate::connector::attempt::Action;
            let action = this.as_mut().project().controller.poll_connect(now);
            match action {
                Action::Connect { key, plan } => {
                    let Some(socket) = plan.socket() else {
                        use dope_net::link::event::ConnectFailure;

                        this.as_mut()
                            .fail_attempt(key, ConnectFailure::NoTarget, driver);
                        continue;
                    };
                    let submitted = {
                        let fields = this.as_mut().project();
                        let app = &*fields.app;
                        fields.pool.submit_socket(
                            0..*fields.primary_capacity,
                            socket,
                            plan,
                            |plan| {
                                use dope_core::io::socket::option::StreamOptions;

                                use crate::connector::connection::State;
                                let (target, options) = plan.into_parts();
                                let configured =
                                    Option::<StreamOptions>::unwrap_or_default(options);
                                (
                                    State::new(
                                        app::Application::connection(app),
                                        <X::Owner as auxiliary::Ownership<'d, ID>>::primary(key),
                                        configured,
                                    ),
                                    target,
                                    options,
                                )
                            },
                            driver,
                        )
                    };
                    match submitted {
                        Ok(open::Outcome::Submitted {
                            key: connection,
                            output,
                        }) => {
                            this.as_mut()
                                .project()
                                .controller
                                .bind(key, connection, output);
                            this.as_mut().submission_succeeded();
                            if !deadline::DeadlinePhase::arm_timeout(
                                this.as_mut(),
                                connection,
                                lifecycle::TimeoutKind::Connect,
                                now,
                                <E::Timing as timing::Policy>::CONNECT_DEADLINE,
                            ) {
                                close::ClosePhase::abort_slot(
                                    this.as_mut(),
                                    connection,
                                    lifecycle::CloseReason::Timeout(
                                        lifecycle::TimeoutKind::Connect,
                                    ),
                                    turn.reborrow(),
                                    driver,
                                );
                            }
                        }
                        Ok(open::Outcome::Deferred { cause, input }) => {
                            this.as_mut()
                                .project()
                                .controller
                                .connect_deferred(key, input, now);
                            let fields = this.as_mut().project();
                            fields
                                .app
                                .open(key, app::OpenOutcome::Deferred(cause), driver);
                            let retry = !matches!(cause, open::Deferred::Capacity);
                            if retry {
                                this.as_mut().defer_submission(now, turn.reborrow(), driver);
                            }
                            return retry;
                        }
                        Err(rejected) => {
                            let (_plan, error) = rejected.into_parts();
                            let fields = this.as_mut().project();
                            fields.controller.kill(key);
                            fields
                                .app
                                .open(key, app::OpenOutcome::Failed(error), driver);
                        }
                    }
                }
                Action::Backoff { min_retry_at } => {
                    if !this.as_ref().project_ref().wake.backoff_armed() {
                        this.as_mut().arm_backoff(min_retry_at);
                    }
                    break;
                }
                Action::Idle => {
                    break;
                }
            }
        }
        false
    }

    fn arm_backoff(self: pin::Pin<&mut Self>, deadline: time::Instant) {
        let ready = self.as_ref().backoff_key();
        let driver = self.as_ref().project_ref().pool.driver();
        let wake = completion::Waker::from_ready(driver, ready);
        self.project()
            .wake
            .as_mut()
            .arm_backoff(driver.scheduler().deadline(deadline), wake);
    }

    fn submission_retry_ready(
        mut self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        if !self.as_ref().project_ref().wake.retry_armed() {
            return true;
        }
        let now = driver.deadline_now();
        let ready = self.as_ref().backoff_key();
        let wake = completion::Waker::from_ready(self.as_ref().project_ref().pool.driver(), ready);
        self.as_mut().project().wake.as_mut().poll_retry(now, wake)
    }

    fn defer_submission(
        mut self: pin::Pin<&mut Self>,
        now: time::Instant,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let attempt = self.as_ref().project_ref().wake.retry_attempt();
        let shift = attempt.min(MAX_SUBMISSION_RETRY_SHIFT);
        let delay = SUBMISSION_RETRY_BASE.saturating_mul(1u32 << u32::from(shift));
        let Some(deadline) = driver
            .driver_ref()
            .scheduler()
            .deadline(now)
            .checked_add(delay)
        else {
            self.shutdown_all(turn, driver);
            return;
        };
        let ready = self.as_ref().backoff_key();
        let wake = completion::Waker::from_ready(self.as_ref().project_ref().pool.driver(), ready);
        self.as_mut().project().wake.as_mut().defer_retry(
            deadline,
            wake,
            MAX_SUBMISSION_RETRY_SHIFT,
        );
    }

    fn submission_succeeded(self: pin::Pin<&mut Self>) {
        self.project().wake.as_mut().retry_succeeded();
    }
}
