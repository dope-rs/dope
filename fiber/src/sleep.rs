use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use pin_project::{pin_project, pinned_drop};

use crate::{Context, Fiber};
use dope::manifold::timer::starved::Waiter;
use dope::manifold::timer::{Ticket, Timer};
use dope::runtime::__private::saturating_deadline;

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
    waiter: Waiter<'d>,
    timer: &'a Timer<'d, ID>,
}

impl<'a, 'd, const ID: u8> Sleep<'a, 'd, ID> {
    pub fn new(timer: &'a Timer<'d, ID>, duration: Duration) -> Self {
        Self {
            deadline: saturating_deadline(Instant::now(), duration),
            ticket: None,
            waiter: Waiter::new(),
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
        let this = self.project();
        let wake = cx.completion_waker();
        match *this.ticket {
            None => match this.timer.try_arm(*this.deadline, wake) {
                Some(t) => {
                    *this.ticket = Some(t);
                    this.timer.unregister_starved(this.waiter.as_ref());
                }
                None => {
                    this.timer
                        .register_starved(this.waiter.as_ref(), *this.deadline, wake);
                }
            },
            Some(t) => {
                if this.timer.is_fired(t) {
                    this.timer.cancel(t);
                    *this.ticket = None;
                    return Poll::Ready(());
                }
                this.timer.replace_waker(t, wake);
            }
        }
        Poll::Pending
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
