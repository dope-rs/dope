use std::{cell::Cell, pin::Pin, ptr::NonNull, task::Poll};

use dope_fiber::{
    abi,
    context::{Continue, PollCall, wake::Waker},
};

#[derive(Debug, Eq, PartialEq)]
pub struct Failure(pub u8);

#[derive(Default)]
pub struct State<'d> {
    pub polls: Cell<usize>,
    pub drops: Cell<usize>,
    pub wake: Cell<Option<Waker<'d>>>,
    address: Cell<Option<NonNull<()>>>,
}

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Producer<'a, 'd> {
    state: &'a State<'d>,
    pending: bool,
    next: Option<Result<u8, Failure>>,
}

impl<'a, 'd> Producer<'a, 'd> {
    pub fn new(state: &'a State<'d>, pending: bool, value: u8) -> Self {
        Self {
            state,
            pending,
            next: Some(Ok(value)),
        }
    }

    pub fn update(self: Pin<&mut Self>, value: Result<u8, Failure>) {
        self.as_ref().observe();
        *self.project().next = Some(value);
    }

    fn observe(self: Pin<&Self>) {
        let address = NonNull::from(self.get_ref()).cast();
        if let Some(previous) = self.state.address.replace(Some(address)) {
            assert_eq!(
                previous, address,
                "stream remains pinned through retirement"
            );
        }
    }
}

impl<'d> abi::Stream<'d> for Producer<'_, 'd> {
    type Item = Result<u8, Failure>;

    fn poll_next(call: PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>> {
        let (stream, context) = call.into_parts();
        stream.as_ref().observe();
        let this = stream.project();
        this.state.polls.set(this.state.polls.get() + 1);
        let wake = context.as_ref().waker();
        if let Some(previous) = this.state.wake.replace(Some(wake)) {
            assert!(previous.completion() == wake.completion());
        }
        if *this.pending {
            *this.pending = false;
            Poll::Pending
        } else {
            let item = this.next.take();
            if item.is_some()
                && let Continue::Permit(permit) = context.as_ref().try_continue()
            {
                permit.run(|| ());
            }
            Poll::Ready(item)
        }
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Producer<'_, '_> {
    fn drop(self: Pin<&mut Self>) {
        self.as_ref().observe();
        self.state.drops.set(self.state.drops.get() + 1);
    }
}
