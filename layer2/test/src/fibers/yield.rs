use std::task::Poll;

use dope_fiber::{abi::Fiber, context::PollCall};

#[derive(Debug, Default)]
pub struct Yield {
    yielded: bool,
    done: bool,
}

impl Yield {
    pub const fn new() -> Self {
        Self {
            yielded: false,
            done: false,
        }
    }
}

impl<'d> Fiber<'d> for Yield {
    type Output = ();
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (mut this, context) = call.into_parts();
        if this.done {
            return Poll::Pending;
        }
        if this.yielded {
            this.done = true;
            Poll::Ready(())
        } else {
            this.yielded = true;
            context.wake();
            Poll::Pending
        }
    }
}
