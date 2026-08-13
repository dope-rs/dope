use std::ops;

use crate::{
    backend::{
        self,
        kqueue::engine::{event, lifecycle},
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
        let cursor = self.backend().poll.next_reactor_cursor();
        let resume_limit = reactors::Fair::share::<{ super::REACTOR_LANES }>(total, 0, cursor);
        {
            let budget = work.budget::<event::ResumeLane>(resume_limit);
            let mut budget = event::Budget::new(budget);
            event::Dispatch::new(self.backend()).resume_pending_with(&mut budget);
        }

        let mut retained = None;
        {
            let limit = work.remaining();
            let budget = work.budget::<event::CompletionLane>(limit);
            let mut budget = event::Budget::new(budget);
            while budget.remaining() != 0 {
                let Some(pending) = self.backend().pending.pop_front() else {
                    break;
                };
                let Some(_credit) = budget.take() else {
                    break;
                };
                let event = {
                    let (backend, drain) = self.backend_drain();
                    resolve_event(backend, &drain, pending)
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

        if work.remaining() != 0 {
            let limit = work.remaining();
            let budget = work.budget::<event::ResumeLane>(limit);
            let mut budget = event::Budget::new(budget);
            event::Dispatch::new(self.backend()).resume_pending_with(&mut budget);
        }
        let backend = self.backend();
        let source_pending = !backend.pending.is_empty() || backend.has_pending_resume();
        let drain = if source_pending || retained.is_some() {
            poll::Drain::Pending
        } else {
            poll::Drain::Drained
        };
        poll::Dispatch::new(drain, retained)
    }
}

fn resolve_event<'d>(
    backend: &mut backend::Kqueue,
    drain: &flight::Drain<'_, 'd>,
    pending: event::Dequeued,
) -> Option<io::Event<'d>> {
    match pending {
        event::Dequeued::Emit(pending) => {
            let completion = pending.into_completion(&mut backend.files, drain);
            Some(io::Event::from_completion(
                completion,
                drain.driver(),
                |len, buffer| backend.recv.region(buffer, len as usize),
            ))
        }
        event::Dequeued::Reclaim(pending) => {
            lifecycle::Control::new(backend).reclaim(pending, drain);
            None
        }
    }
}
