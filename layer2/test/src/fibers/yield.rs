use std::task::Poll;

use dope_fiber::{abi::Fiber, context::PollCall};

#[derive(Debug, Default)]
pub struct Yield {
    yielded: bool,
}

impl Yield {
    pub const fn new() -> Self {
        Self { yielded: false }
    }
}

impl<'d> Fiber<'d> for Yield {
    type Output = ();
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (mut this, context) = call.into_parts();
        if this.yielded {
            Poll::Ready(())
        } else {
            this.yielded = true;
            context.wake();
            Poll::Pending
        }
    }
}
