use std::{io, task, time};

use dope::core::driver::schedule::timer;

use crate::{abi, context};

pub trait TimerExt<'d> {
    fn sleep(&self, duration: time::Duration) -> io::Result<Sleep<'_, 'd>>;
}

impl<'d> TimerExt<'d> for timer::Timer<'d> {
    fn sleep(&self, duration: time::Duration) -> io::Result<Sleep<'_, 'd>> {
        Sleep::new(self, duration)
    }
}

#[pin_project::pin_project(!Unpin)]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Sleep<'a, 'd> {
    #[pin]
    registration: timer::Registration<'a, 'd>,
}

impl<'a, 'd> Sleep<'a, 'd> {
    pub fn new(timer: &'a timer::Timer<'d>, duration: time::Duration) -> io::Result<Self> {
        use dope::core::driver::schedule::timer::Registration;
        let Some(deadline) = timer.deadline_after(duration) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sleep duration exceeds the monotonic clock range",
            ));
        };
        Ok(Self {
            registration: Registration::with_deadline(timer, deadline),
        })
    }
}

impl<'d> abi::Fiber<'d> for Sleep<'_, 'd> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<()> {
        let (this, mut cx) = call.into_parts();
        if !this.as_ref().project_ref().registration.is_armed() {
            use std::task::Poll;
            return Poll::Ready(());
        }
        let wake = cx.as_ref().completion_waker();
        let now = cx.as_mut().driver_access().deadline_now();
        this.project().registration.as_ref().poll(now, wake)
    }
}
