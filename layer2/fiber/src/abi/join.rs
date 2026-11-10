use core::{marker, mem, pin, task};

use crate::{abi, context, task::storage::slots};

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Join<'d, L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    #[pin]
    left: slots::Task<L, L::Output>,
    #[pin]
    right: slots::Task<R, R::Output>,
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
            left: slots::Task::new(left),
            right: slots::Task::new(right),
            next: abi::Side::Left,
            driver: marker::PhantomData,
        }
    }
    fn poll_child<'child, F>(
        child: pin::Pin<&'child mut slots::Task<F, F::Output>>,
        mut context: pin::Pin<&mut context::Context<'_, 'd>>,
        first: &mut bool,
    ) -> Option<slots::raw::Done<'child, F, F::Output>>
    where
        F: abi::Fiber<'d>,
    {
        child.poll(|fiber| {
            // SAFETY: poll admits only the live slot and retains its completed
            // output proof before another child can run.
            if mem::replace(first, false) {
                // SAFETY: this is the join's first live child descent for its
                // admitted PollCall; the slot owns the terminal transition.
                unsafe { context::raw::Live::new(fiber).delegate(context.as_mut()) }
            } else {
                // SAFETY: the live slot owns completion and pays a new credit.
                unsafe { context::raw::Live::new(fiber).poll(context.as_mut()) }
            }
        })
    }
}

impl<'d, L, R> abi::Fiber<'d> for Join<'d, L, R>
where
    L: abi::Fiber<'d>,
    R: abi::Fiber<'d>,
{
    type Output = (L::Output, R::Output);

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.project();
        let side = *this.next;
        *this.next = side.other();
        let mut first = true;
        let (left, right) = match side {
            abi::Side::Left => (
                Self::poll_child(this.left, cx.as_mut(), &mut first),
                Self::poll_child(this.right, cx, &mut first),
            ),
            abi::Side::Right => {
                let right = Self::poll_child(this.right, cx.as_mut(), &mut first);
                (Self::poll_child(this.left, cx, &mut first), right)
            }
        };
        let (Some(left), Some(right)) = (left, right) else {
            return task::Poll::Pending;
        };
        task::Poll::Ready((left.take(), right.take()))
    }
}
