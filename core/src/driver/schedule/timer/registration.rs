use std::{cell, pin, task, time};

use crate::driver::schedule::{
    ready::completion,
    timer::{self, overflow},
};

/// An owning timer registration.
/// Its fixed-slot fast path overflows to an intrusive deadline queue.
/// Timer capacity is a performance bound, never a correctness bound.
#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Registration<'timer, 'd> {
    deadline: cell::Cell<Option<time::Instant>>,
    ticket: cell::Cell<Option<timer::Ticket>>,
    #[pin]
    waiter: overflow::Waiter<'timer, 'd>,
}

impl<'timer, 'd> Registration<'timer, 'd> {
    pub fn new(timer: &'timer timer::Timer<'d>) -> Self {
        Self {
            deadline: cell::Cell::new(None),
            ticket: cell::Cell::new(None),
            waiter: timer.waiter(),
        }
    }

    pub fn with_deadline(timer: &'timer timer::Timer<'d>, deadline: timer::Deadline<'d>) -> Self {
        Self {
            deadline: cell::Cell::new(Some(deadline.at)),
            ticket: cell::Cell::new(None),
            waiter: timer.waiter(),
        }
    }

    pub fn is_armed(self: pin::Pin<&Self>) -> bool {
        self.project_ref().deadline.get().is_some()
    }

    pub fn arm(self: pin::Pin<&Self>, deadline: timer::Deadline<'d>, wake: completion::Waker<'d>) {
        let this = self.project_ref();
        let timer = this.waiter.as_ref().timer();
        if let Some(ticket) = this.ticket.take() {
            timer.cancel(ticket);
        }
        this.waiter.as_ref().unregister();
        this.deadline.set(Some(deadline.at));
        self.register(wake);
    }

    pub fn poll(
        self: pin::Pin<&Self>,
        now: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
    ) -> task::Poll<()> {
        use std::task::Poll;
        let this = self.project_ref();
        let Some(deadline) = this.deadline.get() else {
            return Poll::Pending;
        };
        let timer = this.waiter.as_ref().timer();
        if now.at >= deadline
            || this
                .ticket
                .get()
                .is_some_and(|ticket| timer.is_fired(ticket))
        {
            if let Some(ticket) = this.ticket.take() {
                timer.cancel(ticket);
            }
            this.waiter.as_ref().unregister();
            this.deadline.set(None);
            return Poll::Ready(());
        }
        if let Some(ticket) = this.ticket.get() {
            timer.replace_waker(ticket, wake);
            this.waiter.as_ref().unregister();
        } else {
            self.register(wake);
        }
        Poll::Pending
    }

    pub fn cancel(self: pin::Pin<&Self>) -> bool {
        let this = self.project_ref();
        let was_armed = this.deadline.take().is_some();
        if let Some(ticket) = this.ticket.take() {
            this.waiter.as_ref().timer().cancel(ticket);
        }
        this.waiter.as_ref().unregister();
        was_armed
    }

    fn register(self: pin::Pin<&Self>, wake: completion::Waker<'d>) {
        let this = self.project_ref();
        let Some(deadline) = this.deadline.get() else {
            return;
        };
        let waiter = this.waiter.as_ref();
        match waiter.timer().try_arm(deadline, wake) {
            Ok(armed) => {
                this.ticket.set(Some(armed));
                waiter.unregister();
            }
            Err(wake) => waiter.register(deadline, wake),
        }
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Registration<'_, '_> {
    fn drop(self: pin::Pin<&mut Self>) {
        self.as_ref().cancel();
    }
}
