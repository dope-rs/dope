use std::cell::Cell;

use dope_core::driver::{schedule, schedule::clock};
use o3::base::cell::region;

use crate::common::scenario::dispatch;

#[derive(Default)]
pub struct Quiesce {
    seen: Cell<bool>,
}

impl<'d> dispatch::Hooks<'d, ()> for Quiesce {
    type Park = dispatch::Off;

    fn progress(
        &self,
        _: &(),
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        if self.seen.replace(true) {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::Runnable
        }
    }
}
