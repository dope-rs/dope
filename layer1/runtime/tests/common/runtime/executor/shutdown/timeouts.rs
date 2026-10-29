use std::{cell::Cell, rc::Rc};

use dope_core::driver::{schedule, schedule::clock};
use o3::base::cell::region;

use crate::common::scenario::dispatch;

pub struct Stall {
    pub done: Rc<Cell<bool>>,
    pub gone: Rc<Cell<bool>>,
    pub out: Rc<Cell<bool>>,
}

impl<'d> dispatch::Hooks<'d, ()> for Stall {
    type Park = dispatch::Off;

    fn progress(
        &self,
        _: &(),
        region: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::waiting(region)
    }

    fn finish(&mut self, _: &mut ()) {
        self.done.set(!self.out.get());
    }
}

impl Drop for Stall {
    fn drop(&mut self) {
        self.gone.set(self.done.get());
    }
}

pub struct Output(pub Rc<Cell<bool>>);

impl Drop for Output {
    fn drop(&mut self) {
        self.0.set(true);
    }
}
