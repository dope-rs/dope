use std::pin;

use dope_core::driver::{
    route,
    schedule::{
        ready::{self, completion},
        timer,
    },
};

#[pin_project::pin_project(!Unpin)]
pub(in crate::connector) struct Wake<'d, const ID: u8> {
    #[pin]
    backoff: timer::Registration<'d, 'd>,
    #[pin]
    retry: timer::Registration<'d, 'd>,
    retry_attempt: u8,
    #[pin]
    deadline: timer::Registration<'d, 'd>,
    #[pin]
    slot: ready::Slot<'d, route::KeyTag<ID>>,
    draining: bool,
}

impl<'d, const ID: u8> Wake<'d, ID> {
    pub(in crate::connector) fn new(
        timer: &'d timer::Timer<'d>,
        slot: ready::Slot<'d, route::KeyTag<ID>>,
    ) -> Self {
        use dope_core::driver::schedule::timer::Registration;

        Self {
            backoff: Registration::new(timer),
            retry: Registration::new(timer),
            retry_attempt: 0,
            deadline: Registration::new(timer),
            slot,
            draining: false,
        }
    }

    pub(in crate::connector) fn key(&self) -> ready::Key<'d> {
        self.slot.key()
    }

    pub(in crate::connector) fn target(&self) -> ready::Target<'d> {
        self.slot.target()
    }

    pub(in crate::connector) fn is_draining(self: pin::Pin<&Self>) -> bool {
        *self.project_ref().draining
    }

    pub(in crate::connector) fn backoff_armed(self: pin::Pin<&Self>) -> bool {
        self.project_ref().backoff.is_armed()
    }

    pub(in crate::connector) fn poll_backoff(
        self: pin::Pin<&mut Self>,
        now: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
    ) -> bool {
        matches!(
            self.project().backoff.as_ref().poll(now, wake),
            std::task::Poll::Ready(())
        )
    }

    pub(in crate::connector) fn arm_backoff(
        self: pin::Pin<&mut Self>,
        deadline: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
    ) {
        self.project().backoff.as_ref().arm(deadline, wake);
    }

    pub(in crate::connector) fn retry_armed(self: pin::Pin<&Self>) -> bool {
        self.project_ref().retry.is_armed()
    }

    pub(in crate::connector) fn poll_retry(
        self: pin::Pin<&mut Self>,
        now: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
    ) -> bool {
        self.project().retry.as_ref().poll(now, wake).is_ready()
    }

    pub(in crate::connector) fn retry_attempt(self: pin::Pin<&Self>) -> u8 {
        *self.project_ref().retry_attempt
    }

    pub(in crate::connector) fn defer_retry(
        self: pin::Pin<&mut Self>,
        deadline: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
        max_attempt: u8,
    ) {
        let this = self.project();
        if *this.retry_attempt < max_attempt {
            *this.retry_attempt += 1;
        }
        this.retry.as_ref().arm(deadline, wake);
    }

    pub(in crate::connector) fn retry_succeeded(self: pin::Pin<&mut Self>) {
        *self.project().retry_attempt = 0;
    }

    pub(in crate::connector) fn deadline_armed(self: pin::Pin<&Self>) -> bool {
        self.project_ref().deadline.is_armed()
    }

    pub(in crate::connector) fn set_deadline(
        self: pin::Pin<&mut Self>,
        deadline: Option<timer::Deadline<'d>>,
        wake: completion::Waker<'d>,
    ) {
        let registration = self.project().deadline;
        match deadline {
            Some(deadline) => registration.as_ref().arm(deadline, wake),
            None => {
                registration.as_ref().cancel();
            }
        }
    }

    pub(in crate::connector) fn poll_deadline(
        self: pin::Pin<&mut Self>,
        now: timer::Deadline<'d>,
        wake: completion::Waker<'d>,
    ) -> bool {
        self.project().deadline.as_ref().poll(now, wake).is_ready()
    }

    pub(in crate::connector) fn shutdown(self: pin::Pin<&mut Self>) {
        let this = self.project();
        *this.draining = true;
        this.backoff.as_ref().cancel();
        this.retry.as_ref().cancel();
        this.deadline.as_ref().cancel();
    }
}
