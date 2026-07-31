use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use pin_project::{pin_project, pinned_drop};

use crate::raw::task::CompletionRegistrar;
use crate::{Context, Fiber};
use dope::driver::ready::CompletionWaker;
use dope::manifold::timer::{StarvedWaiter, Ticket, Timer};
use dope::runtime::__private::Deadline;

pub trait TimerExt<'d, const ID: u8 = 0> {
    fn sleep(&self, duration: Duration) -> Sleep<'_, 'd, ID>;
}

impl<'d, const ID: u8> TimerExt<'d, ID> for Timer<'d, ID> {
    fn sleep(&self, duration: Duration) -> Sleep<'_, 'd, ID> {
        Sleep::new(self, duration)
    }
}

#[pin_project(PinnedDrop, !Unpin)]
pub struct Sleep<'a, 'd, const ID: u8 = 0> {
    deadline: Instant,
    ticket: Option<Ticket>,
    #[pin]
    waiter: StarvedWaiter<'d>,
    timer: &'a Timer<'d, ID>,
}

// SAFETY: pinned Sleep owns both possible registrations. Its pinned Drop
// unlinks the waiter and cancels the timer ticket before the task can unbind.
unsafe impl<'timer, 'd, const ID: u8> CompletionRegistrar<'d> for Pin<&mut Sleep<'timer, 'd, ID>> {
    type Output = Poll<()>;

    #[inline(always)]
    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let this = self.project();
        match *this.ticket {
            None => match this.timer.try_arm(*this.deadline, wake) {
                Ok(ticket) => {
                    *this.ticket = Some(ticket);
                    this.timer.unregister_starved(this.waiter.as_ref());
                }
                Err(wake) => {
                    this.timer
                        .register_starved(this.waiter.as_ref(), *this.deadline, wake);
                }
            },
            Some(ticket) => {
                if this.timer.is_fired(ticket) {
                    this.timer.cancel(ticket);
                    *this.ticket = None;
                    return Poll::Ready(());
                }
                this.timer.replace_waker(ticket, wake);
            }
        }
        Poll::Pending
    }
}

impl<'a, 'd, const ID: u8> Sleep<'a, 'd, ID> {
    pub fn new(timer: &'a Timer<'d, ID>, duration: Duration) -> Self {
        Self {
            deadline: Deadline::after(Instant::now(), duration),
            ticket: None,
            waiter: StarvedWaiter::new(),
            timer,
        }
    }

    fn cancel_ticket(ticket: &mut Option<Ticket>, timer: &Timer<'d, ID>) {
        if let Some(t) = ticket.take() {
            timer.cancel(t);
        }
    }
}

impl<'d, const ID: u8> Fiber<'d> for Sleep<'_, 'd, ID> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        if Instant::now() >= self.as_ref().get_ref().deadline {
            let this = self.project();
            this.timer.unregister_starved(this.waiter.as_ref());
            Self::cancel_ticket(this.ticket, this.timer);
            return Poll::Ready(());
        }
        cx.as_ref().register_completion(self)
    }
}

#[pinned_drop]
impl<const ID: u8> PinnedDrop for Sleep<'_, '_, ID> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        this.timer.unregister_starved(this.waiter.as_ref());
        Self::cancel_ticket(this.ticket, this.timer);
    }
}
