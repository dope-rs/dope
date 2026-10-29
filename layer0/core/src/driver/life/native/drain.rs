use dope_sys::{Budget as _, engine::life::Step};
use o3::mem::quota;
use state::lanes;

use crate::{
    driver,
    driver::{route::queues, schedule::state},
    io::fd::domain::retire,
};

pub(crate) enum Lane {}

enum Record<'d> {
    Descriptor(retire::Pending<'d>),
    Route(queues::Retired<'d>),
}

#[derive(Clone, Copy)]
enum Cursor {
    Route,
    Descriptor,
}

pub(in crate::driver) struct Drain<'a, 'scope, 'd> {
    context: &'a mut driver::Context<'scope, 'd>,
    cursor: Cursor,
}

pub(in crate::driver) struct Final<'a, 'scope, 'd> {
    context: &'a mut driver::Context<'scope, 'd>,
}

impl<'a, 'scope, 'd> Drain<'a, 'scope, 'd> {
    pub(in crate::driver) fn reclaim(
        ctx: &'a mut driver::Context<'scope, 'd>,
        budget: &mut lanes::Budget<'_, 'd, Lane>,
    ) {
        let mut drain = Self {
            context: ctx,
            cursor: Cursor::Route,
        };
        loop {
            let admit = budget.admit(|| drain.select());
            let record = match admit {
                quota::Admission::Item(record) => record,
                quota::Admission::Empty | quota::Admission::Exhausted => break,
            };
            if let Step::Blocked = drain.record(record) {
                break;
            }
        }
    }

    fn select(&mut self) -> Option<Record<'d>> {
        let driver = self.context.driver_ref();
        let files = driver.descriptors().retirement();
        if matches!(self.cursor, Cursor::Descriptor)
            && let Some(pending) = files.pending()
        {
            return Some(Record::Descriptor(pending));
        }
        match queues::Pending::pop(driver) {
            Some(id) => Some(Record::Route(id)),
            None => match self.cursor {
                Cursor::Route => files.pending().map(Record::Descriptor),
                Cursor::Descriptor => None,
            },
        }
    }

    fn record(&mut self, record: Record<'d>) -> Step {
        self.cursor = match self.cursor {
            Cursor::Route => Cursor::Descriptor,
            Cursor::Descriptor => Cursor::Route,
        };
        let ctx = &mut *self.context;
        match record {
            Record::Descriptor(pending) => ctx.native().resolve().release(pending),
            Record::Route(id) => {
                ctx.storage.release(id);
                Step::Advanced
            }
        }
    }
}

impl<'a, 'scope, 'd> Final<'a, 'scope, 'd> {
    pub(in crate::driver) fn new(ctx: &'a mut driver::Context<'scope, 'd>) -> Self {
        Self { context: ctx }
    }

    pub(in crate::driver) fn all(self) {
        let driver = self.context.driver_ref();
        while let Some(id) = queues::Pending::pop(driver) {
            self.context.storage.release(id);
        }
    }
}
