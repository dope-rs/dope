use std::{pin, task};

use dope_core::driver::{
    self, retained,
    route::Identity,
    schedule,
    schedule::{clock, ready::wake},
};
use o3::mem::quota;

use crate::{
    app::env,
    client::{
        app, attempt,
        conn::engine::{self, ops::opens},
        side::mode::Mode,
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::client) enum Source {
    Idle,
    Deferred,
}

impl<'p, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X> engine::Machine<'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Application<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    pub(in crate::client::conn::engine) fn poll_dials(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Source
    where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>: opens::Phase<'d, ID, A, S, E>,
    {
        let now = driver.driver_ref().clock().now();
        let mut this = self;
        if !this.as_ref().project_ref().shutdown.is_open() {
            return Source::Idle;
        }
        let mut woke = {
            let target = this.as_ref().backoff();
            let wake = wake::Waker::from(target);
            this.as_mut()
                .project()
                .drive
                .as_mut()
                .poll_backoff(now, wake)
                .is_ready()
        };
        if !this.as_ref().project_ref().controller.is_pending() && !woke {
            return Source::Idle;
        }
        let cap = this.as_ref().project_ref().links.capacity();
        for _ in 0..cap.entries().get() {
            use opens::Step;
            let step = turn.reborrow().maint().run(|| {
                match opens::Phase::poll(this.as_mut(), now, driver) {
                    Step::Idle => None,
                    step => Some(step),
                }
            });
            let action = match step {
                quota::Admission::Item(action) => action,
                quota::Admission::Empty => break,
                quota::Admission::Exhausted => {
                    let target = this.as_ref().backoff();
                    if woke {
                        // The consumed alarm still owns the unadmitted opening.
                        this.as_mut()
                            .project()
                            .drive
                            .as_mut()
                            .arm_backoff(now.into(), target.into());
                    }
                    if woke || this.as_ref().project_ref().controller.is_pending() {
                        target.wake();
                    }
                    return Source::Deferred;
                }
            };
            match action {
                Step::Continue => woke = false,
                Step::Stop => return Source::Idle,
                Step::Retry => {
                    return this.defer_retry(now);
                }
                Step::Backoff(at) => {
                    let target = this.as_ref().backoff();
                    let wake = wake::Waker::from(target);
                    this.as_mut().project().drive.as_mut().arm_backoff(at, wake);
                    break;
                }
                Step::Idle => break,
            }
        }
        Source::Idle
    }

    pub(in crate::client) fn retry_ready(
        mut self: pin::Pin<&mut Self>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> task::Poll<()> {
        if !self.as_ref().project_ref().drive.has_retry() {
            return task::Poll::Ready(());
        }
        let now = driver.driver_ref().clock().now();
        let target = self.as_ref().backoff();
        let wake = wake::Waker::from(target);
        self.as_mut().project().drive.as_mut().poll_retry(now, wake)
    }

    pub(in crate::client) fn defer_retry(
        mut self: pin::Pin<&mut Self>,
        now: clock::Moment<'d>,
    ) -> Source {
        let target = self.as_ref().backoff();
        let wake = wake::Waker::from(target);
        self.as_mut()
            .project()
            .drive
            .as_mut()
            .defer_retry(now, wake);
        Source::Deferred
    }
}
