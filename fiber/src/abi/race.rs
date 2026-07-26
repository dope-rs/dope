use core::pin::Pin;
use core::task::Poll;

use pin_project::pin_project;

use super::Fiber;
use crate::raw::task::Context;

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

#[pin_project]
pub struct Race<L, R> {
    #[pin]
    left: L,
    #[pin]
    right: R,
}

impl<L, R> Race<L, R> {
    pub const fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<'d, L, R> Fiber<'d> for Race<L, R>
where
    L: Fiber<'d>,
    R: Fiber<'d>,
{
    type Output = Either<L::Output, R::Output>;

    fn poll(self: Pin<&mut Self>, mut context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(output) = Fiber::poll(this.left, context.as_mut()) {
            return Poll::Ready(Either::Left(output));
        }
        Fiber::poll(this.right, context).map(Either::Right)
    }
}
