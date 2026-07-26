use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

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

pub struct Sleep<'a, 'd, const ID: u8 = 0> {
    deadline: Instant,
    ticket: Option<Ticket>,
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

    fn poll_step(&mut self, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            let waiter = unsafe { Pin::new_unchecked(&self.waiter) };
            self.timer.unregister_starved(waiter);
            self.cancel_step();
            return Poll::Ready(());
        }
        let wake = cx.completion_waker();
        match self.ticket {
            None => match self.timer.try_arm(self.deadline, wake) {
                Some(t) => {
                    self.ticket = Some(t);
                    let waiter = unsafe { Pin::new_unchecked(&self.waiter) };
                    self.timer.unregister_starved(waiter);
                }
                None => {
                    let waiter = unsafe { Pin::new_unchecked(&self.waiter) };
                    self.timer.register_starved(waiter, self.deadline, wake);
                }
            },
            Some(t) => {
                if self.timer.is_fired(t) {
                    self.timer.cancel(t);
                    self.ticket = None;
                    return Poll::Ready(());
                }
                self.timer.replace_waker(t, wake);
            }
        }
        Poll::Pending
    }

    fn cancel_step(&mut self) {
        if let Some(t) = self.ticket.take() {
            self.timer.cancel(t);
        }
    }
}

impl<'d, const ID: u8> Fiber<'d> for Sleep<'_, 'd, ID> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        let this = unsafe { self.get_unchecked_mut() };
        this.poll_step(cx)
    }
}

impl<const ID: u8> Drop for Sleep<'_, '_, ID> {
    fn drop(&mut self) {
        let waiter = unsafe { Pin::new_unchecked(&self.waiter) };
        self.timer.unregister_starved(waiter);
        self.cancel_step();
    }
}
