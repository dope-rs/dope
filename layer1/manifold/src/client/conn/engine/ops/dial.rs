use std::pin;

use dope_core::driver::{
    self, retained,
    route::Identity,
    schedule,
    schedule::{clock, ready::wake},
};
use dope_net::wire;
use o3::mem::quota;

use crate::{
    app::env,
    client::{
        app, attempt,
        conn::engine::{self, liveness, ops::opens},
        side::mode::Mode,
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::client) enum Source {
    Idle,
    Deferred,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::client) enum Retry {
    Ready,
    Pending,
}

pub(in crate::client) struct Dial<'a, 'p, 'w, 'd: 'w, ID: Identity + 'd, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID>,
{
    machine: pin::Pin<&'a mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
}

impl<'p, 'a, 'w, 'd: 'p + 'w, ID: Identity + 'd, A, S, E, X> Dial<'a, 'p, 'w, 'd, ID, A, S, E, X>
where
    A: app::Lifecycle<'w, 'd, ID> + app::Scheduling<'d, ID>,
    S: attempt::Control<'d, E::Transport, ID>,
    E: env::Spec<Wire = A::Wire>,
    E::Transport: dope_net::Transport,
    X: Mode<'d, ID> + liveness::Schedule<'d, ID, <E::Transport as dope_net::Transport>::Prepared>,
{
    pub(in crate::client) fn new(
        machine: pin::Pin<&'a mut engine::Machine<'p, 'w, 'd, ID, A, S, E, X>>,
    ) -> Self {
        Self { machine }
    }

    pub(in crate::client::conn::engine) fn poll(
        self,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Source
    where
        engine::Machine<'p, 'w, 'd, ID, A, S, E, X>:
            opens::Phase<'d, ID, A, S, E, A::Deferred, <A::Wire as wire::Spec>::OpenFailure>,
    {
        let now = driver.driver_ref().clock().now();
        let mut this = self.machine;
        if !this.as_ref().project_ref().shutdown.is_open() {
            return Source::Idle;
        }
        let woke = {
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
                quota::Admission::Exhausted => return Source::Deferred,
            };
            match action {
                Step::Continue => {}
                Step::Stop => return Source::Idle,
                Step::Retry => {
                    return Dial { machine: this }.defer_retry(now);
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

    pub(in crate::client) fn retry_ready(mut self, driver: &mut driver::Context<'_, 'd>) -> Retry {
        if !self.machine.as_ref().project_ref().drive.has_retry() {
            return Retry::Ready;
        }
        let now = driver.driver_ref().clock().now();
        let target = self.machine.as_ref().backoff();
        let wake = wake::Waker::from(target);
        if self
            .machine
            .as_mut()
            .project()
            .drive
            .as_mut()
            .poll_retry(now, wake)
            .is_ready()
        {
            Retry::Ready
        } else {
            Retry::Pending
        }
    }

    pub(in crate::client) fn defer_retry(mut self, now: clock::Moment<'d>) -> Source {
        let target = self.machine.as_ref().backoff();
        let wake = wake::Waker::from(target);
        self.machine
            .as_mut()
            .project()
            .drive
            .as_mut()
            .defer_retry(now, wake);
        Source::Deferred
    }

    pub(in crate::client) fn finish_retry(self) {
        self.machine.project().drive.as_mut().reset_retry();
    }
}
