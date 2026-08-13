use std::{ops, process};

use crate::{
    backend::{
        self, fixed,
        uring::{
            self,
            engine::{completions, tuning},
            ring,
        },
    },
    driver::{
        self, flight,
        ops::{poll, reactors},
        schedule,
    },
    io,
};

impl<'d> poll::Source<'d> for driver::Context<'_, 'd> {
    fn dispatch(
        &mut self,
        work: schedule::Reactor<'_, 'd>,
        mut dispatch: impl FnMut(io::Event<'d>, &mut Self) -> ops::ControlFlow<io::Event<'d>>,
    ) -> poll::Dispatch<'d> {
        let work = reactors::Backend::prepare(self, work);
        let total = work.remaining();
        let cursor = self.backend().next_reactor_cursor();
        let maintenance_limit = reactors::Fair::share::<{ super::REACTOR_LANES }>(total, 0, cursor);
        let maintenance = {
            let mut budget = work.budget::<super::Maintenance>(maintenance_limit);
            super::maintain(self, &mut budget)
        };

        let source = ring::Drain::begin(&mut self.backend().ring);
        let mut retained = None;
        {
            let limit = work.remaining();
            let mut budget = work.budget::<super::Completion>(limit);
            while let schedule::Admission::Item(item) = budget.admit_with(|| {
                source
                    .next(&mut self.backend().ring)
                    .map(completions::Cqe::new)
            }) {
                let event = {
                    let (backend, drain) = self.backend_drain();
                    let backend::Uring {
                        ring,
                        tuning,
                        fixed_slots,
                        ..
                    } = backend;
                    resolve_event(ring.buffers().provided(), tuning, fixed_slots, &drain, item)
                };
                let Some(event) = event else {
                    continue;
                };
                match dispatch(event, self) {
                    ops::ControlFlow::Continue(()) => {}
                    ops::ControlFlow::Break(event) => {
                        retained = Some(event);
                        break;
                    }
                }
            }
        }
        let source_pending = source.finish(&mut self.backend().ring);

        if maintenance == super::MaintenanceState::Pending && work.remaining() != 0 {
            let limit = work.remaining();
            let mut budget = work.budget::<super::Maintenance>(limit);
            let _ = super::maintain(self, &mut budget);
        }
        self.backend().ring.buffers().provided().flush();
        let drain = if source_pending || retained.is_some() {
            poll::Drain::Pending
        } else {
            poll::Drain::Drained
        };
        poll::Dispatch::new(drain, retained)
    }
}

fn resolve_event<'d>(
    provided: &mut uring::ffi::ProvidedRing,
    tuning: &mut tuning::Table,
    fixed_slots: &mut fixed::Slots,
    drain: &flight::Drain<'_, 'd>,
    item: completions::Cqe,
) -> Option<io::Event<'d>> {
    let driver = drain.driver();
    let disposition =
        completions::Resolver::new(provided, tuning, fixed_slots, drain).resolve(item);
    match disposition {
        completions::Disposition::Consumed(buffer) => {
            if let Some(buffer) = buffer {
                provided.defer(buffer);
            }
            None
        }
        completions::Disposition::Public(completion) => Some(io::Event::from_completion(
            completion,
            driver,
            |len, buffer| provided.region(buffer, buffer_len(len)),
        )),
        completions::Disposition::Closed(closed) => {
            if closed.result() < 0 || closed.work().retires_slot() {
                process::abort();
            }
            closed.settle(fixed_slots, driver);
            None
        }
    }
}

fn buffer_len(len: u32) -> usize {
    let Ok(len) = usize::try_from(len) else {
        process::abort();
    };
    len
}
