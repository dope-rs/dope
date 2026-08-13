use std::{cell, pin, time};

use o3::collections::intrusive::avl;

use crate::driver::schedule::{credits, ready::completion, timer};

mod sealed;

pub(super) use sealed::Queue;

pub(super) struct Overflow<'d> {
    queue: Queue<'d>,
}

impl<'d> Overflow<'d> {
    pub(super) fn new() -> Self {
        Self {
            queue: Queue::new(),
        }
    }

    fn register(
        &self,
        waiter: pin::Pin<&avl::raw::Entry<State<'d>>>,
        deadline: time::Instant,
        wake: completion::Waker<'d>,
    ) {
        self.queue.register(waiter, deadline, wake);
    }

    fn unregister(&self, waiter: pin::Pin<&avl::raw::Entry<State<'d>>>) -> bool {
        self.queue.unregister(waiter)
    }

    pub(super) fn wake_min(&self) {
        self.queue.wake_min();
    }

    pub(super) fn expire(
        &self,
        now: time::Instant,
        budget: &mut credits::Budget<'_, 'd, timer::Lane>,
    ) {
        self.queue.expire(now, budget)
    }

    pub(super) fn min_deadline(&self) -> Option<time::Instant> {
        self.queue.min_deadline()
    }
}

struct State<'d> {
    wake: completion::Slot<'d>,
    deadline: cell::Cell<time::Instant>,
}

impl State<'_> {
    fn new() -> Self {
        use std::{cell::Cell, time::Instant};

        use crate::driver::schedule::ready::completion::Slot;

        Self {
            wake: Slot::empty(),
            deadline: Cell::new(Instant::now()),
        }
    }
}

#[pin_project::pin_project(PinnedDrop)]
pub(super) struct Waiter<'timer, 'd> {
    timer: &'timer timer::Timer<'d>,
    #[pin]
    entry: avl::raw::Entry<State<'d>>,
}

impl<'timer, 'd> Waiter<'timer, 'd> {
    pub(super) fn new(timer: &'timer timer::Timer<'d>) -> Self {
        use o3::collections::intrusive::avl::raw::Entry;
        Self {
            timer,
            entry: Entry::new(State::new()),
        }
    }

    pub(super) fn register(
        self: pin::Pin<&Self>,
        deadline: time::Instant,
        wake: completion::Waker<'d>,
    ) {
        let this = self.project_ref();
        this.timer.overflow.register(this.entry, deadline, wake);
    }

    pub(super) fn unregister(self: pin::Pin<&Self>) {
        let this = self.project_ref();
        if this.timer.overflow.unregister(this.entry) && this.timer.has_free_slot() {
            this.timer.overflow.wake_min();
        }
    }

    pub(super) fn timer(self: pin::Pin<&Self>) -> &'timer timer::Timer<'d> {
        self.project_ref().timer
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Waiter<'_, '_> {
    fn drop(self: pin::Pin<&mut Self>) {
        self.as_ref().unregister();
    }
}
