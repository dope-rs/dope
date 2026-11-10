use std::{convert::Infallible, ops::ControlFlow, pin::Pin, task::Poll};

use crate::{abi, context::PollCall, task::storage::fixed::schedule::result::Error};

/// # Safety
/// A nonterminal poll must leave `F` valid for another call without repolling completed children.
/// `held` must lend the exact flag that prevents polling while an item is retained.
pub unsafe trait Mode<'d, F> {
    type Output;
    type Item;
    const INITIAL: Self;

    fn poll(
        &mut self,
        call: PollCall<'_, '_, 'd, F>,
    ) -> Poll<ControlFlow<Self::Output, Self::Item>>;

    fn held<'a>(&'a mut self, _fiber: Pin<&'a mut F>) -> Result<&'a mut bool, Error> {
        Err(Error::Absent)
    }
}

// SAFETY: completion terminates the owning slot; fixed fibers never lend a held item.
unsafe impl<'d, F: abi::Fiber<'d>> Mode<'d, F> for () {
    type Output = F::Output;
    type Item = Infallible;
    const INITIAL: Self = ();

    fn poll(&mut self, call: PollCall<'_, '_, 'd, F>) -> Poll<ControlFlow<F::Output, Infallible>> {
        F::poll(call).map(ControlFlow::Break)
    }
}

pub struct Stream {
    held: bool,
}

// SAFETY: EOF terminates the slot, and the held flag gates every subsequent item poll.
unsafe impl<'d, F: abi::Stream<'d>> Mode<'d, F> for Stream {
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

    fn held<'a>(&'a mut self, _fiber: Pin<&'a mut F>) -> Result<&'a mut bool, Error> {
        Ok(&mut self.held)
    }
}
