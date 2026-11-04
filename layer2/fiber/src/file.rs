//! Owned file operations backed by an installed manifold Reader.

use std::task;

use o3::mem::Rejected;

use crate::{
    abi::{self, machine},
    context,
    manifold::file::read,
};

enum Stage<'p, 'd> {
    Request(read::Request<'d>),
    Ticket(read::Ticket<'p, 'd>),
}

/// Cancellation drops only the reply ticket. The Reader keeps native inputs.
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Operation<'p, 'd> {
    port: &'p read::Port<'d>,
    stage: machine::Machine<Stage<'p, 'd>>,
}

impl<'p, 'd> Operation<'p, 'd> {
    pub fn new(port: &'p read::Port<'d>, request: read::Request<'d>) -> Self {
        Self {
            port,
            stage: machine::Machine::new(Stage::Request(request)),
        }
    }
}

impl<'d> abi::Fiber<'d> for Operation<'_, 'd> {
    type Output = Result<read::Completion<'d>, Rejected<read::Request<'d>, read::Admission>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let waiter = cx.as_ref().completion();
        this.stage.step(|stage| {
            let ticket = match stage {
                Stage::Ticket(ticket) => ticket,
                Stage::Request(request) => match this.port.try_request(request) {
                    Ok(ticket) => ticket,
                    Err(cause) => return machine::Step::Complete(task::Poll::Ready(Err(cause))),
                },
            };
            match ticket.poll(waiter) {
                read::Poll::Pending(ticket) => {
                    machine::Step::Suspend(Stage::Ticket(ticket), task::Poll::Pending)
                }
                read::Poll::Ready(completion) => {
                    machine::Step::Complete(task::Poll::Ready(Ok(completion)))
                }
            }
        })
    }
}
