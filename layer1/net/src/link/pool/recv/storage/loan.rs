use dope_core::driver::receive::credit;
use o3::depot::queue::pool;

use crate::link::{event, pool::recv::storage::queue};

pub struct Loan<'a, 'd, R = ()> {
    events: pool::Lane<'a, event::Completion<'d, R>>,
    credit: credit::Slot<'a, 'd>,
}

impl<'a, 'd, R> Loan<'a, 'd, R> {
    pub(crate) fn new(
        events: pool::Lane<'a, event::Completion<'d, R>>,
        credit: credit::Slot<'a, 'd>,
    ) -> Self {
        Self { events, credit }
    }

    pub(in crate::link::pool) fn get(&self) -> credit::Slot<'a, 'd> {
        self.credit
    }
}

impl<'d, R> queue::Ops<'d, R> for Loan<'_, 'd, R> {
    fn prepare_pop(&mut self) -> Option<pool::Front<'_, event::Completion<'d, R>, ()>> {
        queue::Ops::prepare_pop(&mut self.events)
    }

    fn has_queued(&self) -> bool {
        queue::Ops::has_queued(&self.events)
    }
}

impl<'d, R> queue::Write<'d, R> for Loan<'_, 'd, R> {
    fn prepare_push(&mut self) -> Option<queue::Vacant<'_, 'd, R>> {
        queue::Write::prepare_push(&mut self.events)
    }
}
