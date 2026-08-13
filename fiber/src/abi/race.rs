use core::task;

use crate::{abi, context};

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
/// Resolves with the first completed child, preferring `left` when both run.
/// A budget-skipped child resumes first on the next poll.
pub struct Race<L, R> {
    #[pin]
    left: L,
    #[pin]
    right: R,
    resume: abi::Side,
}

const _: () = assert!(core::mem::size_of::<Race<(), ()>>() == core::mem::size_of::<abi::Side>());

impl<L, R> Race<L, R> {
    pub const fn new(left: L, right: R) -> Self {
        Self {
            left,
            right,
            resume: abi::Side::Left,
        }
    }
}

impl<'d, L, R> abi::Fiber<'d> for Race<L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    type Output = Either<L::Output, R::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use core::task::Poll;

        let (this, mut context) = call.into_parts();
        let this = this.project();
        let first = *this.resume;
        match first {
            abi::Side::Left => {
                match context.as_mut().try_poll(this.left) {
                    Some(Poll::Ready(output)) => return Poll::Ready(Either::Left(output)),
                    Some(Poll::Pending) => {}
                    None => return Poll::Pending,
                }
                *this.resume = abi::Side::Right;
                match context.try_poll(this.right) {
                    Some(Poll::Ready(output)) => Poll::Ready(Either::Right(output)),
                    Some(Poll::Pending) => {
                        *this.resume = abi::Side::Left;
                        Poll::Pending
                    }
                    None => Poll::Pending,
                }
            }
            abi::Side::Right => {
                match context.as_mut().try_poll(this.right) {
                    Some(Poll::Ready(output)) => return Poll::Ready(Either::Right(output)),
                    Some(Poll::Pending) => {}
                    None => return Poll::Pending,
                }
                *this.resume = abi::Side::Left;
                context
                    .try_poll(this.left)
                    .unwrap_or(Poll::Pending)
                    .map(Either::Left)
            }
        }
    }
}
