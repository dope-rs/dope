use std::cell::Cell;
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

use pin_project::{pin_project, pinned_drop};

use super::{Ticket, Timer, Waiter};
use crate::driver::ready::CompletionWaker;

/// An owning, starvation-safe timer registration.
///
/// The registration keeps the timer ticket linear and falls back to the
/// driver's intrusive deadline queue when all fixed timer slots are busy.
/// Consequently timer capacity is a performance bound, never a correctness
/// bound.
#[pin_project(PinnedDrop, !Unpin)]
pub struct Registration<'timer, 'd> {
    deadline: Cell<Option<Instant>>,
    ticket: Cell<Option<Ticket>>,
    #[pin]
    waiter: Waiter<'timer, 'd>,
}

impl<'timer, 'd> Registration<'timer, 'd> {
    pub fn new(timer: &'timer Timer<'d>) -> Self {
        Self {
            deadline: Cell::new(None),
            ticket: Cell::new(None),
            waiter: timer.waiter(),
        }
    }

    pub fn with_deadline(timer: &'timer Timer<'d>, deadline: Instant) -> Self {
        Self {
            deadline: Cell::new(Some(deadline)),
            ticket: Cell::new(None),
            waiter: timer.waiter(),
        }
    }

    pub fn is_armed(self: Pin<&Self>) -> bool {
        self.project_ref().deadline.get().is_some()
    }

    pub fn arm(self: Pin<&Self>, deadline: Instant, wake: CompletionWaker<'d>) {
        let this = self.project_ref();
        let timer = this.waiter.as_ref().timer();
        if let Some(ticket) = this.ticket.take() {
            timer.cancel(ticket);
        }
        this.waiter.as_ref().unregister();
        this.deadline.set(Some(deadline));
        self.register(wake);
    }

    pub fn poll(self: Pin<&Self>, now: Instant, wake: CompletionWaker<'d>) -> Poll<()> {
        let this = self.project_ref();
        let Some(deadline) = this.deadline.get() else {
            return Poll::Pending;
        };
        let timer = this.waiter.as_ref().timer();
        if now >= deadline
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

    pub fn cancel(self: Pin<&Self>) -> bool {
        let this = self.project_ref();
        let was_armed = this.deadline.take().is_some();
        if let Some(ticket) = this.ticket.take() {
            this.waiter.as_ref().timer().cancel(ticket);
        }
        this.waiter.as_ref().unregister();
        was_armed
    }

    fn register(self: Pin<&Self>, wake: CompletionWaker<'d>) {
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

#[pinned_drop]
impl PinnedDrop for Registration<'_, '_> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().cancel();
    }
}
