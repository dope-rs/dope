use std::{cell::Cell, time::Duration};

use dope::{
    core::driver::schedule::clock,
    runtime::{control, shutdown},
};
use dope_quic::{
    client::{self, auth},
    conn::life::options::Options,
    errors,
};

use super::{
    traces::Immediate,
    writable::{Callback, Connected},
};

pub struct Config<'a>(pub &'a Cell<Option<client::AttemptId>>);

impl client::ConfigSource for Config<'_> {
    fn config<'scope, 'd>(
        &'scope mut self,
        attempt: client::AttemptScope<'scope, 'd>,
    ) -> Option<Options> {
        self.0.set(Some(attempt.id()));
        Some(Options::default())
    }
}

pub type Protocol = Connected<for<'a> fn(Result<client::SlotId, Callback<'a>>)>;

pub fn protocol() -> Protocol {
    Connected::new(|_| {})
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub struct App<'d, 'a> {
    #[pin]
    #[manifold(control)]
    pub first: client::Dialer<'d, 'd, 1, Protocol, Immediate, auth::Owned, Config<'a>>,
    #[pin]
    #[manifold(control)]
    pub second: client::Dialer<'d, 'd, 2, Protocol, Immediate, auth::Owned, Config<'a>>,
    #[dispatcher(state)]
    pub attempts: &'a [Cell<Option<client::AttemptId>>; 2],
    #[dispatcher(state)]
    pub checked: &'a Cell<bool>,
    #[dispatcher(state)]
    pub stop: Option<shutdown::Trigger>,
    #[dispatcher(schedule)]
    pub timeout: clock::Deadline<'d>,
}

impl<'d, 'a> App<'d, 'a> {
    fn coordinate(mut this: app_coord::Fields<'_, 'd, 'a>) -> control::Flow {
        if this.stop.is_none() {
            return control::Flow::Idle;
        }
        if let [Some(first), Some(second)] = this.attempts.each_ref().map(Cell::get) {
            assert_eq!(first.slot(), second.slot());
            assert_eq!(first.generation(), second.generation());
            assert_ne!(first, second);
            let mut first_client = this.first.at(this.step.clock());
            assert_eq!(first_client.with_attempt(first, |_| ()), Ok(()));
            assert_eq!(
                first_client.with_attempt(second, |_| ()),
                Err(errors::Access::InvalidHandle)
            );
            let mut second_client = this.second.at(this.step.clock());
            assert_eq!(second_client.with_attempt(second, |_| ()), Ok(()));
            assert_eq!(
                second_client.with_attempt(first, |_| ()),
                Err(errors::Access::InvalidHandle)
            );
            this.checked.set(true);
        }
        if this.checked.get() || this.timeout.is_due(this.step.now()) {
            this.stop
                .take()
                .expect("shutdown owner")
                .fire()
                .expect("shutdown signal");
        }
        control::Flow::Idle
    }
}

pub const TIMEOUT: Duration = Duration::from_secs(2);
