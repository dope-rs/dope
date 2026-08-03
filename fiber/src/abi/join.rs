use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;

use pin_project::pin_project;

use super::Fiber;
use crate::Context;

#[pin_project]
pub struct Join<'d, L, R>
where
    L: Fiber<'d>,
    R: Fiber<'d>,
{
    #[pin]
    left: L,
    #[pin]
    right: R,
    left_output: Option<L::Output>,
    right_output: Option<R::Output>,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, L, R> Join<'d, L, R>
where
    L: Fiber<'d>,
    R: Fiber<'d>,
{
    pub fn new(left: L, right: R) -> Self {
        Self {
            left,
            right,
            left_output: None,
            right_output: None,
            driver: PhantomData,
        }
    }
}

impl<'d, L, R> Fiber<'d> for Join<'d, L, R>
where
    L: Fiber<'d>,
    R: Fiber<'d>,
{
    type Output = (L::Output, R::Output);

    fn poll(self: Pin<&mut Self>, mut cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let mut this = self.project();
        if this.left_output.is_none()
            && let Poll::Ready(output) = this.left.as_mut().poll(cx.as_mut())
        {
            *this.left_output = Some(output);
        }
        if this.right_output.is_none()
            && let Poll::Ready(output) = this.right.as_mut().poll(cx.as_mut())
        {
            *this.right_output = Some(output);
        }
        match (this.left_output.take(), this.right_output.take()) {
            (Some(left), Some(right)) => Poll::Ready((left, right)),
            (left, right) => {
                *this.left_output = left;
                *this.right_output = right;
                Poll::Pending
            }
        }
    }
}
