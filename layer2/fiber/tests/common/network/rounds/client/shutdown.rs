use std::{pin::Pin, task::Poll};

use dope::net::{tcp::Tcp, wire::Identity};
use dope_core::driver::route::KeyTag;
use dope_fiber::{
    abi::{Fiber, PollFn, Task},
    context::Continue,
    net::client::Machine,
};

pub const PAYLOAD: &[u8] = b"pending";

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'q, 's, 't, 'd> {
    #[pin]
    #[manifold]
    pub(crate) first: Machine<'p, 's, 'd, 'd, KeyTag<0>, Tcp, Identity>,
    #[pin]
    #[manifold]
    pub(crate) second: Machine<'q, 't, 'd, 'd, KeyTag<1>, Tcp, Identity>,
}

pub fn retain_pending<'a, 'd: 'a, F>(
    fiber: F,
) -> impl Fiber<'d, Output = (Pin<Box<Task<F>>>, Poll<F::Output>)> + 'a
where
    F: Fiber<'d> + 'a,
{
    let mut fiber = Some(Box::pin(Task::new(fiber)));
    PollFn::admitted(move |admit| {
        let Some(mut child) = fiber.take() else {
            return Poll::Pending;
        };
        let (poll, context) = admit.poll(child.as_mut());
        while let Continue::Permit(_) = context.as_ref().try_continue() {}
        Poll::Ready((child, poll))
    })
}
