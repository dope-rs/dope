//! Owned file operations backed by an installed manifold Reader.

use std::{ops::ControlFlow, task};

use o3::mem::{Rejected, Return};

use crate::{
    abi::{self, machine},
    context,
    manifold::file::read,
};

enum Stage<'p, 'd, R: Return> {
    Request(read::Request<R>),
    Ticket(read::Ticket<'p, 'd, R>),
}

/// Cancellation drops only the reply ticket. The Reader keeps native inputs.
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Operation<'p, 'd, R: Return = read::Release<'d>> {
    port: &'p read::Port<'d, R>,
    stage: machine::Machine<Stage<'p, 'd, R>>,
}

impl<'p, 'd, R: Return> Operation<'p, 'd, R> {
    pub fn new(port: &'p read::Port<'d, R>, request: read::Request<R>) -> Self {
        Self {
            port,
            stage: machine::Machine::new(Stage::Request(request)),
        }
    }
}

// No field has a pinned projection; the request moves into the Reader's stable slot.
impl<R: Return> Unpin for Operation<'_, '_, R> {}

impl<'d, R: Return<Item = read::Buffer<'d>>> abi::Fiber<'d> for Operation<'_, 'd, R> {
    type Output = Result<read::Completion<R>, Rejected<read::Request<R>, read::Admission>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let waiter = cx.as_ref().completion();
        this.stage.step(|stage| {
            let ticket = match stage {
                Stage::Ticket(ticket) => ticket,
                Stage::Request(request) => match this.port.try_request(request) {
                    Ok(ticket) => ticket,
                    Err(cause) => return ControlFlow::Break(Err(cause)),
                },
            };
            match ticket.poll(Some(waiter)) {
                read::Poll::Pending(ticket) => ControlFlow::Continue(Stage::Ticket(ticket)),
                read::Poll::Ready(completion) => ControlFlow::Break(Ok(completion)),
            }
        })
    }
}
