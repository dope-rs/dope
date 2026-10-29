use std::{task::Poll, time::Duration};

use dope_fiber::{abi::Fiber, context::PollCall, task::time::sleep};

pub(in crate::fibers) struct Delay<'d> {
    duration: Option<Duration>,
    timer: Option<sleep::Sleep<'d>>,
}

impl Delay<'_> {
    pub(in crate::fibers) const fn new(duration: Duration) -> Self {
        Self {
            duration: Some(duration),
            timer: None,
        }
    }
}

impl<'d> Fiber<'d> for Delay<'d> {
    type Output = Result<(), sleep::Error>;
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        if let Some(duration) = this.duration.take() {
            this.timer = Some(sleep::Sleep::new(
                context.as_mut().driver().driver_ref().timer(),
                duration,
            )?);
        }
        let Some(timer) = &mut this.timer else {
            return Poll::Pending;
        };
        let polled = timer.poll_sleep(context);
        if polled.is_ready() {
            this.timer = None;
        }
        polled.map(Ok)
    }
}
