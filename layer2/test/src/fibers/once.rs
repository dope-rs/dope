use std::task::Poll;

use dope_fiber::{
    abi::{Fiber, Task},
    context::PollCall,
};

pub(in crate::fibers) struct Once<F> {
    fiber: Option<Task<F>>,
}

impl<F> Once<F> {
    pub(in crate::fibers) fn new(fiber: F) -> Self {
        Self {
            fiber: Some(Task::new(fiber)),
        }
    }
}

impl<'d, F: Fiber<'d>> Fiber<'d> for Once<F> {
    type Output = Option<F::Output>;
    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        // SAFETY: the private child is never moved and its owner has no Drop.
        let mut slot = unsafe { this.map_unchecked_mut(|this| &mut this.fiber) };
        let Some(fiber) = slot.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let (result, _) = admit.poll(fiber);
        slot.set(None);
        Poll::Ready(match result {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        })
    }
}
