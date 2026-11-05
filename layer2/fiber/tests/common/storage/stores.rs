use std::{cell::Cell, pin::Pin, ptr, ptr::NonNull, rc::Rc, task::Poll};

use dope_fiber::{
    abi::{Fiber, Stream},
    context::PollCall,
};

pub struct Domain;

pub struct Empty<T>(pub T);

impl<T> Stream<'_> for Empty<T> {
    type Item = ();

    fn poll_next(_: PollCall<'_, '_, '_, Self>) -> Poll<Option<()>> {
        Poll::Ready(None)
    }
}

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Pinned {
    pub(crate) address: Cell<Option<ptr::NonNull<Self>>>,
    pub(crate) observed: Rc<Cell<(usize, bool)>>,
}

impl Stream<'_> for Pinned {
    type Item = ();

    fn poll_next(call: PollCall<'_, '_, '_, Self>) -> Poll<Option<()>> {
        let fiber = call.into_fiber();
        let address = NonNull::from(fiber.as_ref().get_ref());
        let this = fiber.project();
        if let Some(bound) = this.address.replace(Some(address)) {
            let (drops, stable) = this.observed.get();
            this.observed.set((drops, stable && bound == address));
        }
        Poll::Pending
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Pinned {
    fn drop(self: Pin<&mut Self>) {
        let address = NonNull::from(self.as_ref().get_ref());
        let this = self.project();
        let (drops, stable) = this.observed.get();
        this.observed
            .set((drops + 1, stable && this.address.get() == Some(address)));
    }
}

pub struct DropOutput {
    pub(crate) drops: Rc<Cell<usize>>,
}

impl Drop for DropOutput {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[pin_project::pin_project]
pub struct Complete {
    #[pin]
    pub(crate) pinned: Pinned,
    pub(crate) output: Option<DropOutput>,
    pub(crate) pending: bool,
}

impl Fiber<'_> for Complete {
    type Output = DropOutput;

    fn poll(call: PollCall<'_, '_, '_, Self>) -> Poll<DropOutput> {
        let this = call.into_fiber().project();
        if std::mem::replace(this.pending, false) {
            this.pinned
                .address
                .set(Some(NonNull::from(this.pinned.as_ref().get_ref())));
            Poll::Pending
        } else {
            Poll::Ready(this.output.take().expect("one completion"))
        }
    }
}
