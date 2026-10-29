use core::task;

use crate::{abi, context};

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
/// Resolves with the first completed child, preferring `left` when both run.
/// A budget-skipped child resumes first on the next poll.
/// Completion ends the owner's polling authority for this race.
#[derive(Debug)]
pub struct Race<L, R> {
    #[pin]
    left: L,
    #[pin]
    right: R,
    resume: abi::Side,
}

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
        match *this.resume {
            abi::Side::Left => {
                // SAFETY: this live race owns both unfinished children and
                // transfers its existing admission to the first child.
                if let Poll::Ready(output) =
                    unsafe { context::raw::Live::new(this.left).delegate(context.as_mut()) }
                {
                    return Poll::Ready(Either::Left(output));
                }
                *this.resume = abi::Side::Right;
                // SAFETY: this child is unfinished; Ready ends the race's poll.
                match unsafe { context::raw::Live::new(this.right).try_poll(context) } {
                    context::PollResult::Polled(Poll::Ready(output)) => {
                        Poll::Ready(Either::Right(output))
                    }
                    context::PollResult::Polled(Poll::Pending) => {
                        *this.resume = abi::Side::Left;
                        Poll::Pending
                    }
                    context::PollResult::Exhausted => Poll::Pending,
                }
            }
            abi::Side::Right => {
                // SAFETY: the skipped child is unfinished and receives the
                // existing admission; Ready ends the race's poll.
                if let Poll::Ready(output) =
                    unsafe { context::raw::Live::new(this.right).delegate(context.as_mut()) }
                {
                    return Poll::Ready(Either::Right(output));
                }
                *this.resume = abi::Side::Left;
                // SAFETY: this child is unfinished; Ready ends the race's poll.
                unsafe { context::raw::Live::new(this.left).poll(context) }.map(Either::Left)
            }
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Race<(), ()>>() == core::mem::size_of::<abi::Side>());
    assert!(core::mem::size_of::<Option<Race<(), ()>>>() == core::mem::size_of::<Race<(), ()>>());
};

#[derive(Debug)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}
