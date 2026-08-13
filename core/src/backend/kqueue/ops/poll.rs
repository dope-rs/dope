use std::{io, time};

use crate::{
    backend::kqueue::engine::event,
    driver::{
        self,
        ops::{poll, reactors},
        schedule,
    },
};

struct Work<'turn, 'd> {
    reactor: reactors::Backend<'turn, 'd>,
    completion_floor: usize,
}

impl<'turn, 'd> Work<'turn, 'd> {
    fn new(context: &mut driver::Context<'_, 'd>, work: schedule::Reactor<'turn, 'd>) -> Self {
        let reactor = reactors::Backend::prepare(context, work);
        let total = reactor.remaining();
        let cursor = context.backend().poll.next_reactor_cursor();
        let resume_limit = reactors::Fair::share::<{ super::REACTOR_LANES }>(total, 0, cursor);
        let completion_floor = reactors::Fair::share::<{ super::REACTOR_LANES }>(total, 2, cursor);
        let work = Self {
            reactor,
            completion_floor,
        };
        work.resume(context, resume_limit);
        work
    }

    fn wait(
        &self,
        context: &mut driver::Context<'_, '_>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()> {
        let change_limit = context.backend().poll.changes.len().min(
            self.reactor
                .remaining()
                .saturating_sub(self.completion_floor),
        );
        {
            let changes = self.reactor.budget::<event::ChangeLane>(change_limit);
            let mut changes = event::Budget::new(changes);
            let completion_limit = self
                .reactor
                .remaining()
                .min(context.backend().pending.remaining_capacity());
            let completions = self
                .reactor
                .budget::<event::CompletionLane>(completion_limit);
            let mut completions = event::Budget::new(completions);
            context
                .backend()
                .wait(timeout, &mut changes, &mut completions)?;
        }
        self.resume(context, self.reactor.remaining());
        Ok(())
    }

    fn resume(&self, context: &mut driver::Context<'_, '_>, limit: usize) {
        let limit = limit.min(context.backend().pending.remaining_capacity());
        let budget = self.reactor.budget::<event::ResumeLane>(limit);
        let mut budget = event::Budget::new(budget);
        event::Dispatch::new(context.backend()).resume_pending_with(&mut budget);
    }
}

impl<'d> poll::Poll<'d> for driver::Context<'_, 'd> {
    fn commit(&mut self, work: schedule::Reactor<'_, 'd>) -> io::Result<poll::Commit> {
        let work = Work::new(self, work);
        if !self.backend().poll.changes.is_empty() {
            work.wait(self, Some(time::Duration::ZERO))?;
        }
        self.backend().poll.check()?;
        Ok(if self.backend().poll.changes.is_empty() {
            poll::Commit::Drained
        } else {
            poll::Commit::Pending
        })
    }

    fn wait(
        &mut self,
        work: schedule::Reactor<'_, 'd>,
        timeout: Option<time::Duration>,
    ) -> io::Result<()> {
        let work = Work::new(self, work);
        let deferred_maintenance = self.driver_ref().maintenance().has_deferred_maintenance();
        let has_ready = self.driver_ref().ready().has_ready();
        let backend = self.backend();
        let must_not_block = !backend.pending.is_empty()
            || deferred_maintenance
            || has_ready
            || (work.reactor.remaining() == 0 && backend.has_pending_resume());
        let timeout = if must_not_block {
            Some(time::Duration::ZERO)
        } else {
            timeout
        };
        work.wait(self, timeout)
    }
}
