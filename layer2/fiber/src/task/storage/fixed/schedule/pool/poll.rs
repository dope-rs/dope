use std::{convert::Infallible, ops::ControlFlow, task::Poll};

use crate::{abi, context::PollCall};

pub trait Mode<'d, F> {
    type Output;
    type Item;
    const INITIAL: Self;

    fn poll(
        &mut self,
        call: PollCall<'_, '_, 'd, F>,
    ) -> Poll<ControlFlow<Self::Output, Self::Item>>;
}

impl<'d, F: abi::Fiber<'d>> Mode<'d, F> for () {
    type Output = F::Output;
    type Item = Infallible;
    const INITIAL: Self = ();

    fn poll(&mut self, call: PollCall<'_, '_, 'd, F>) -> Poll<ControlFlow<F::Output, Infallible>> {
        F::poll(call).map(ControlFlow::Break)
    }
}

pub struct Stream {
    pub(super) held: bool,
}

impl<'d, F: abi::Stream<'d>> Mode<'d, F> for Stream {
    type Output = ();
    type Item = F::Item;
    const INITIAL: Self = Self { held: false };

    fn poll(&mut self, call: PollCall<'_, '_, 'd, F>) -> Poll<ControlFlow<(), F::Item>> {
        if self.held {
            return Poll::Pending;
        }
        F::poll_next(call).map(|item| match item {
            Some(item) => {
                self.held = true;
                ControlFlow::Continue(item)
            }
            None => ControlFlow::Break(()),
        })
    }
}
