use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::driver::ready::CompletionWaker;
use dope::driver::timer::{Registration, Timer};
use dope::runtime::__private::Deadline;
use pin_project::pin_project;

use crate::raw::task::CompletionRegistrar;
use crate::{Context, Fiber};

pub trait TimerExt<'d> {
    fn sleep(&self, duration: Duration) -> Sleep<'_, 'd>;
}

impl<'d> TimerExt<'d> for Timer<'d> {
    fn sleep(&self, duration: Duration) -> Sleep<'_, 'd> {
        Sleep::new(self, duration)
    }
}

#[pin_project(!Unpin)]
pub struct Sleep<'a, 'd> {
    #[pin]
    registration: Registration<'a, 'd>,
}

// SAFETY: the pinned Registration owns and removes every retained completion
// handle before the task can unbind.
unsafe impl<'timer, 'd> CompletionRegistrar<'d> for Pin<&mut Sleep<'timer, 'd>> {
    type Output = Poll<()>;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        if !self.as_ref().project_ref().registration.is_armed() {
            return Poll::Ready(());
        }
        self.project()
            .registration
            .as_ref()
            .poll(Instant::now(), wake)
    }
}

impl<'a, 'd> Sleep<'a, 'd> {
    pub fn new(timer: &'a Timer<'d>, duration: Duration) -> Self {
        Self {
            registration: Registration::with_deadline(
                timer,
                Deadline::after(Instant::now(), duration),
            ),
        }
    }
}

impl<'d> Fiber<'d> for Sleep<'_, 'd> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        cx.as_ref().register_completion(self)
    }
}
