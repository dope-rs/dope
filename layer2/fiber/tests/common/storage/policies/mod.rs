use std::{cell::Cell, task::Poll};

use dope::core::driver::schedule::ready::wake::Waker;
use dope_fiber::{
    abi,
    context::{self, Continue},
};

pub mod owner;
pub mod prepare;

pub struct Domain;

pub struct Rearm<'a, 'd>(pub Option<(&'a Cell<usize>, &'a Cell<Option<Waker<'d>>>)>);

impl<'d> abi::Fiber<'d> for Rearm<'_, 'd> {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (fiber, context) = call.into_parts();
        if let Some((polls, wake)) = fiber.0 {
            polls.set(polls.get() + 1);
            wake.set(Some(context.as_ref().completion()));
        } else {
            while let Continue::Permit(_) = context.as_ref().try_continue() {}
        }
        Poll::Pending
    }
}
