use core::{marker, pin, task};
use std::process;

use crate::{
    abi::{self, slot},
    context,
};

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Join<'d, L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    #[pin]
    left: slot::Slot<L, L::Output>,
    #[pin]
    right: slot::Slot<R, R::Output>,
    next: abi::Side,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, L, R> Join<'d, L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    pub fn new(left: L, right: R) -> Self {
        Self {
            left: slot::Slot::live(left),
            right: slot::Slot::live(right),
            next: abi::Side::Left,
            driver: marker::PhantomData,
        }
    }
}

fn poll_child<'d, F>(
    mut child: pin::Pin<&mut slot::Slot<F, F::Output>>,
    mut context: pin::Pin<&mut context::Context<'_, 'd>>,
) -> bool
where
    F: abi::Fiber<'d>,
{
    if !child.is_live() {
        return true;
    }
    let Some(fiber) = child.as_mut().as_live() else {
        return true;
    };
    let Some(poll) = context.as_mut().try_poll(fiber) else {
        return false;
    };
    if let task::Poll::Ready(output) = poll {
        child.complete(output);
    }
    true
}

impl<'d, L, R> abi::Fiber<'d> for Join<'d, L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    type Output = (L::Output, R::Output);

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let mut this = this.project();
        if this.left.is_vacant() || this.right.is_vacant() {
            process::abort();
        }
        let first = *this.next;
        *this.next = first.other();
        let admitted = match first {
            abi::Side::Left => {
                poll_child(this.left.as_mut(), cx.as_mut())
                    && poll_child(this.right.as_mut(), cx.as_mut())
            }
            abi::Side::Right => {
                poll_child(this.right.as_mut(), cx.as_mut())
                    && poll_child(this.left.as_mut(), cx.as_mut())
            }
        };
        if !admitted {
            return task::Poll::Pending;
        }
        if !this.left.is_done() || !this.right.is_done() {
            return task::Poll::Pending;
        }
        let left = this.left.as_mut().take_output();
        let right = this.right.as_mut().take_output();
        task::Poll::Ready((left, right))
    }
}
