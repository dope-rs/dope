use std::cell::Cell;

use dope::{
    core::driver::schedule::clock,
    runtime::{control, shutdown},
};
use dope_quic::client::{self, auth};

use super::{
    traces::Immediate,
    writable::{Callback, Connected},
};

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub struct App<'d, 'a, F: for<'event> FnMut(Result<client::SlotId, Callback<'event>>) + Unpin> {
    #[pin]
    #[manifold(control)]
    pub client: client::Dialer<'d, 'd, 1, Connected<F>, Immediate, auth::Owned>,
    #[dispatcher(state)]
    pub settled: &'a Cell<usize>,
    #[dispatcher(state)]
    pub checked: &'a Cell<bool>,
    #[dispatcher(state)]
    pub stop: Option<shutdown::Trigger>,
    #[dispatcher(schedule)]
    pub timeout: clock::Deadline<'d>,
}

impl<'d, 'a, F: for<'event> FnMut(Result<client::SlotId, Callback<'event>>) + Unpin>
    App<'d, 'a, F>
{
    fn coordinate(this: app_coord::Fields<'_, 'd, 'a, F>) -> control::Flow {
        if this.stop.is_none() {
            return control::Flow::Idle;
        }
        if this.settled.get() != 0 {
            assert_eq!(this.client.failure(), None);
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
