use std::{pin::Pin, task::Poll, time::Duration};

use dope_fiber::{
    abi::{Fiber, Task},
    context::PollCall,
    task::time::sleep,
};

use crate::{
    fibers::{Delay, Wait},
    harness::signals::Progress,
};

pub(in crate::fibers) struct Until<'a, 'd, G: ?Sized> {
    gate: Option<&'a G>,
    want: u32,
    timer: Task<Delay<'d>>,
}

impl<'a, G: ?Sized> Until<'a, '_, G> {
    pub(in crate::fibers) const fn new(gate: &'a G, want: u32, duration: Duration) -> Self {
        Self {
            gate: Some(gate),
            want,
            timer: Task::new(Delay::new(duration)),
        }
    }
}

impl<'d, G: Progress + ?Sized> Fiber<'d> for Until<'_, 'd, G> {
    type Output = Result<Wait, sleep::Error>;
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut admit) = call.into_admit();
        let this = this.get_mut();
        let Some(gate) = this.gate else {
            return Poll::Pending;
        };
        if gate.hits() >= this.want {
            this.gate = None;
            return Poll::Ready(Ok(Wait::Reached));
        }
        admit.context().wake();
        let (polled, _) = admit.poll(Pin::new(&mut this.timer));
        let polled = polled.map(|result| result.map(|()| Wait::Deadline));
        if polled.is_ready() {
            this.gate = None;
        }
        polled
    }
}
