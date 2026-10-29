use std::task;

use crate::{
    abi, context,
    core::driver::{retained, schedule::app},
    task::storage::{
        set,
        set::{
            ready::{Queue, core},
            slot, views,
        },
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Side {
    First,
    Second,
}

pub(in crate::task::storage::set) struct Drain<'a, 'd, F: abi::Fiber<'d>, M, D, G> {
    view: views::View<'a, 'd, F, M, D, G>,
    token: slot::Token<'a, F, M>,
    side: Side,
}

impl Side {
    pub(super) const fn flip(self) -> Self {
        match self {
            Self::First => Self::Second,
            Self::Second => Self::First,
        }
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Drain<'a, 'd, F, M, D, G> {
    pub(super) fn new(
        view: views::View<'a, 'd, F, M, D, G>,
        token: slot::Token<'a, F, M>,
        side: Side,
    ) -> Self {
        Self { view, token, side }
    }

    pub(in crate::task::storage::set) fn peek(
        &mut self,
    ) -> Option<core::Queued<'_, 'a, 'd, F, M, D, G>> {
        Queue::as_pin(&self.view.header().ready).backend().front(
            &self.view,
            &mut self.token,
            self.side,
        )
    }

    pub(in crate::task::storage::set) fn drive(
        mut self,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> set::Length {
        let mut done = set::Length::ZERO;
        loop {
            let (mut running, permit) = match work.next(&mut self) {
                app::Next::Item(running, permit) => (running, permit),
                app::Next::Empty => return done,
                app::Next::Exhausted(queued) => {
                    queued.wake();
                    return done;
                }
            };
            let (parent, wake) = running.wakers();
            let poll = context::Context::with_waker(
                wake,
                parent,
                work,
                driver.reborrow(),
                |mut context| {
                    running.poll(|fiber| {
                        // SAFETY: Running retains the live pinned slot and is
                        // consumed by completion before another poll is possible.
                        unsafe { context::raw::Live::new(fiber).with(context.as_mut(), permit) }
                    })
                },
            );
            let task::Poll::Ready(output) = poll else {
                continue;
            };
            running.complete(output);
            done.0 += 1;
        }
    }
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> app::Drain for Drain<'a, 'd, F, M, D, G> {
    type Candidate<'b>
        = core::Queued<'b, 'a, 'd, F, M, D, G>
    where
        Self: 'b;
    type Item<'b>
        = core::Running<'b, 'a, 'd, F, M, D, G>
    where
        Self: 'b;

    fn next_when<P>(
        &mut self,
        take: impl FnOnce() -> Option<P>,
    ) -> app::Selection<Self::Item<'_>, P, Self::Candidate<'_>> {
        let queued = self.peek()?;
        let Some(permit) = take() else {
            return Some(Err(queued));
        };
        Some(Ok((queued.start(), permit)))
    }
}

impl<'d, F: abi::Fiber<'d>, M, D, G> Drop for Drain<'_, 'd, F, M, D, G> {
    fn drop(&mut self) {
        let queue = Queue::as_pin(&self.view.header().ready);
        let core = queue.backend();
        if core.is_empty_on(self.side) {
            core.paused.set(None);
            if let Some(queued) = core.front(&self.view, &mut self.token, self.side.flip()) {
                queued.wake();
            } else if let Some(index) = queue.slot().peek() {
                // SAFETY: the retained topology front proves this live runnable root.
                unsafe { slot::raw::Queued::new(self.view.scheduled(index)) }.wake();
            }
        }
    }
}
