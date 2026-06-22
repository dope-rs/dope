use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::{Fiber, Holding};
use crate::backend;
use crate::manifold::timer::{HasTimer, Ticket, Timer};

impl<'d, const ID: u8> Holding<'d, Timer<ID>> {
    pub fn sleep(self, d: Duration) -> Fiber<'d, Sleep<'d, ID>> {
        Fiber::new(Sleep::new(self, d))
    }
}

impl<'d, T: HasTimer> Holding<'d, T> {
    pub fn sleep(self, d: Duration) -> Fiber<'d, Sleep<'d, 0>> {
        let pinned = self.hold().timer();
        let raw = NonNull::from(unsafe { Pin::into_inner_unchecked(pinned) });
        // SAFETY: HasTimer::timer returns a Timer<0> pinned within the same `self` this Holding brands for 'd, so the pointer is valid for 'd.
        let timer = unsafe { Holding::<'d, Timer<0>>::from_raw(raw) };
        timer.sleep(d)
    }
}

pub struct Sleep<'d, const ID: u8 = 0> {
    deadline: Instant,
    ticket: Option<Ticket>,
    timer: Holding<'d, Timer<ID>>,
}

impl<'d, const ID: u8> Sleep<'d, ID> {
    pub fn new(timer: Holding<'d, Timer<ID>>, d: Duration) -> Self {
        Self {
            deadline: Instant::now() + d,
            ticket: None,
            timer,
        }
    }

    fn cancel_ticket(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.timer.hold().get_mut().cancel(ticket);
        }
    }
}

impl<'d, const ID: u8> Future for Sleep<'d, ID> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let now = Instant::now();
        if now >= this.deadline {
            this.cancel_ticket();
            return Poll::Ready(());
        }
        let caller = backend::park::WakeRef::verified(cx.waker());
        let h = this.timer.hold();
        let timer = h.get_mut();
        match this.ticket {
            None => match timer.try_arm(this.deadline, caller) {
                Some(t) => this.ticket = Some(t),
                None => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            },
            Some(ticket) => {
                if timer.is_fired(ticket) {
                    timer.cancel(ticket);
                    this.ticket = None;
                    return Poll::Ready(());
                }
                timer.replace_caller(ticket, caller);
            }
        }
        Poll::Pending
    }
}

impl<'d, const ID: u8> Drop for Sleep<'d, ID> {
    fn drop(&mut self) {
        self.cancel_ticket();
    }
}
