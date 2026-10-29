//! Zero-state projection of the current fiber poll's driver clock.

use std::task;

use crate::{abi, context, core::driver::schedule::clock};

/// Clock projection constructors for the active driver turn.
pub enum Clock {}

impl Clock {
    pub const fn now() -> Now {
        Now
    }
}

/// A zero-sized, one-poll clock projection. It uses no TLS, allocation, wake
/// slot, or scheduler credit beyond the poll which already executes it.
#[must_use = "a clock projection does nothing unless it is driven"]
pub struct Now;

impl<'d> abi::Fiber<'d> for Now {
    type Output = clock::Moment<'d>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let mut context = call.into_context();
        task::Poll::Ready(context.as_mut().driver().driver_ref().clock().now())
    }
}

const _: () = assert!(std::mem::size_of::<Now>() == 0);
