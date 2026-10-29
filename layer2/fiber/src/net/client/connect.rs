use std::task;

use dope::{
    core::driver::route::{Identity, KeyTag},
    net::{self, wire},
};

use crate::{
    abi::{self, machine},
    context,
    manifold::client::attempt::queue::mailbox,
    net::{Io, client},
};

enum Stage<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity> {
    Init(T::Addr),
    Ticket(client::Ticket<'scope, 'w, 'd, T, W, ID>),
}

/// A connect operation that owns any waker registration made for its dial ticket.
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Connect<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity = KeyTag<0>> {
    host: client::Handle<'scope, 'w, 'd, T, W, ID>,
    stage: machine::Machine<Stage<'scope, 'w, 'd, T, W, ID>>,
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity>
    Connect<'scope, 'w, 'd, T, W, ID>
{
    pub(super) fn new(host: client::Handle<'scope, 'w, 'd, T, W, ID>, addr: T::Addr) -> Self {
        Self {
            host,
            stage: machine::Machine::new(Stage::Init(addr)),
        }
    }
}

impl<T: net::Transport, W: wire::Spec, ID: Identity> Unpin for Connect<'_, '_, '_, T, W, ID> {}

impl<T: net::Transport, W: wire::Spec, ID: Identity> Drop for Connect<'_, '_, '_, T, W, ID> {
    fn drop(&mut self) {
        let Some(Stage::Ticket(lease)) = self.stage.take() else {
            return;
        };
        if let mailbox::Cancellation::Settled(outcome) = lease.cancel() {
            self.host.port.reclaim(outcome);
        }
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity> abi::Fiber<'d>
    for Connect<'scope, 'w, 'd, T, W, ID>
{
    type Output = client::Connected<'scope, 'w, 'd, W, ID>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let wake = cx.as_ref().completion();
        this.stage.step(|stage| {
            let lease = match stage {
                Stage::Ticket(lease) => lease,
                Stage::Init(addr) => match this.host.port.dial(addr) {
                    Ok(lease) => lease,
                    Err(error) => {
                        return machine::Step::Complete(task::Poll::Ready(Err(error)));
                    }
                },
            };
            let (outcome, lease) = match lease.poll(wake) {
                mailbox::Poll::Ready(outcome, lease) => (outcome, lease),
                mailbox::Poll::Pending(lease) => {
                    return machine::Step::Suspend(Stage::Ticket(lease), task::Poll::Pending);
                }
            };
            let error = match outcome {
                client::Outcome::Connected(conn) => {
                    let io = Io::new(&this.host.port.conns, conn);
                    lease.commit();
                    return machine::Step::Complete(task::Poll::Ready(Ok(io)));
                }
                client::Outcome::Activation => client::ConnectError::Activation,
                client::Outcome::Transport(cause) => client::ConnectError::Transport(cause),
                client::Outcome::Open(cause) => client::ConnectError::Open(cause),
            };
            lease.cancel();
            machine::Step::Complete(task::Poll::Ready(Err(error)))
        })
    }
}
