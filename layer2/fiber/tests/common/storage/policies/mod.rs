use std::task::Poll;

use dope_fiber::context::Continue;

pub mod owner;
pub mod prepare;

use std::task;

use dope_fiber::{abi, context};

pub struct Domain;

pub struct Rearm;

impl<'d> abi::Fiber<'d> for Rearm {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let context = call.into_context();
        while let Continue::Permit(_) = context.as_ref().try_continue() {}
        Poll::Pending
    }
}
