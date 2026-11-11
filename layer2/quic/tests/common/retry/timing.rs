use std::{cell::Cell, time};

use dope::{
    core::driver::schedule::clock,
    runtime::{control, shutdown},
};
use dope_quic::{
    client::{self, auth, protocol},
    conn::life::options::Options,
    errors,
};

use super::writable::{Callback, Connected};

pub struct Config<'a> {
    pub defer: bool,
    pub started: &'a Cell<Option<(client::AttemptId, time::Instant)>>,
}

impl client::ConfigSource for Config<'_> {
    fn config<'scope, 'd>(
        &'scope mut self,
        attempt: client::AttemptScope<'scope, 'd>,
    ) -> Option<Options> {
        assert!(
            self.started
                .replace(Some((attempt.id(), attempt.started_at().into())))
                .is_none(),
            "a failed retry deadline seals further attempts"
        );
        (!self.defer).then(Options::default)
    }
}

pub struct Backoff<'a>(pub &'a Cell<usize>);

impl client::Backoff for Backoff<'_> {
    fn retry_after<'scope>(&'scope self, retry: client::RetryScope<'scope>) -> time::Duration {
        assert_eq!(retry.previous_id().generation(), 1);
        assert_eq!(retry.next_id().generation(), 2);
        self.0.set(self.0.get() + 1);
        time::Duration::MAX
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub struct App<'d, 'a, F: for<'event> FnMut(Result<client::SlotId, Callback<'event>>) + Unpin> {
    #[pin]
    #[manifold(control)]
    pub client: client::Dialer<'d, 'd, 1, Connected<F>, Backoff<'a>, auth::Owned, Config<'a>>,
    #[dispatcher(state)]
    pub started: &'a Cell<Option<(client::AttemptId, time::Instant)>>,
    #[dispatcher(state)]
    pub closed_at: &'a Cell<Option<time::Instant>>,
    #[dispatcher(state)]
    pub failure: &'a Cell<Option<protocol::Failure>>,
    #[dispatcher(state)]
    pub stop: Option<shutdown::Trigger>,
    #[dispatcher(schedule)]
    pub timeout: clock::Deadline<'d>,
}

impl<'d, 'a, F: for<'event> FnMut(Result<client::SlotId, Callback<'event>>) + Unpin>
    App<'d, 'a, F>
{
    fn coordinate(mut this: app_coord::Fields<'_, 'd, 'a, F>) -> control::Flow {
        if this.stop.is_none() {
            return control::Flow::Idle;
        }
        if let Some(failure) = this.client.failure() {
            if let protocol::Failure::Timing {
                cause: dope_quic::conn::Timing::Deadline { at, .. },
                ..
            } = failure
            {
                assert!(at <= this.step.now().into());
            }
            this.failure.set(Some(failure));
            this.client.at(this.step.clock()).restart_all();
            assert_eq!(this.client.failure(), Some(failure));
            this.stop
                .take()
                .expect("shutdown owner")
                .fire()
                .expect("shutdown signal");
            return control::Flow::Idle;
        }
        if let Some((attempt, _)) = this.started.get()
            && this.closed_at.get().is_none()
        {
            this.closed_at.set(Some(this.step.now().into()));
            this.client
                .at(this.step.clock())
                .with_attempt(attempt, |mut conn| {
                    conn.lifecycle().close(errors::Code::ZERO, Vec::new());
                })
                .expect("close the exact handshaking attempt");
        }
        assert!(
            !this.timeout.is_due(this.step.now()),
            "retry deadline failure must be observable"
        );
        control::Flow::Idle
    }
}
