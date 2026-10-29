use dope_core::driver::receive::credit;
use o3::depot::queue::pool;

use crate::link::{event, pool::recv::storage::queue};

pub struct Loan<'a, 'd> {
    events: pool::Lane<'a, event::Completion<'d>>,
    credit: credit::Slot<'a, 'd>,
}

impl<'a, 'd> Loan<'a, 'd> {
    pub(crate) fn new(
        events: pool::Lane<'a, event::Completion<'d>>,
        credit: credit::Slot<'a, 'd>,
    ) -> Self {
        Self { events, credit }
    }

    pub(in crate::link::pool) fn get(&self) -> credit::Slot<'_, 'd> {
        self.credit
    }
}

impl<'d> queue::Ops<'d> for Loan<'_, 'd> {
    fn prepare_pop(&mut self) -> Option<pool::Front<'_, event::Completion<'d>, ()>> {
        queue::Ops::prepare_pop(&mut self.events)
    }

    fn has_queued(&self) -> bool {
        queue::Ops::has_queued(&self.events)
    }
}

impl<'d> queue::Write<'d> for Loan<'_, 'd> {
    fn prepare_push<T>(&mut self, done: T) -> queue::Push<'_, 'd, T> {
        queue::Write::prepare_push(&mut self.events, done)
    }
}
