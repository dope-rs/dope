use std::{io, mem, task};

use dope::{
    core::driver::schedule::ready::completion,
    manifold::connector::{attempt::queue, connection},
};
use transport::wire;

use crate::{
    abi, context,
    net::{self, connector},
};

enum Stage<'scope, 'd, T: transport::Transport, const ID: u8> {
    Init {
        addr: T::Addr,
        config: T::StreamConfig,
    },
    Ticket(queue::Lease<'scope, 'd, T, ID>),
    Done,
}

/// A connect operation that owns any waker registration made for its dial ticket.
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Connect<'scope, 'd, T: transport::Transport, W: wire::Wire, const ID: u8 = 0> {
    host: connector::Handle<'scope, 'd, T, W, ID>,
    stage: Stage<'scope, 'd, T, ID>,
}

impl<'scope, 'd, T: transport::Transport, W: wire::Wire, const ID: u8>
    Connect<'scope, 'd, T, W, ID>
{
    fn poll_completion(
        &mut self,
        lease: queue::Lease<'scope, 'd, T, ID>,
        wake: completion::Waker<'d>,
    ) -> task::Poll<io::Result<net::Io<'scope, 'd, W, connection::Id<'d, ID>>>> {
        match self.host.port.resolve(lease.id(), wake) {
            task::Poll::Ready(Ok(connection)) => {
                let io = net::Io::new(&self.host.port.connections, connection);
                lease.commit();
                self.stage = Stage::Done;
                task::Poll::Ready(Ok(io))
            }
            task::Poll::Ready(Err(error)) => {
                drop(lease);
                self.stage = Stage::Done;
                task::Poll::Ready(Err(error))
            }
            task::Poll::Pending => {
                self.stage = Stage::Ticket(lease);
                task::Poll::Pending
            }
        }
    }

    pub(super) fn new(
        host: connector::Handle<'scope, 'd, T, W, ID>,
        addr: T::Addr,
        config: T::StreamConfig,
    ) -> Self {
        Self {
            host,
            stage: Stage::Init { addr, config },
        }
    }
}

impl<T: transport::Transport, W: wire::Wire, const ID: u8> Unpin for Connect<'_, '_, T, W, ID> {}

impl<T: transport::Transport, W: wire::Wire, const ID: u8> Drop for Connect<'_, '_, T, W, ID> {
    fn drop(&mut self) {
        let Stage::Ticket(lease) = &self.stage else {
            return;
        };
        self.host.port.cancel(lease.id());
    }
}

impl<'scope, 'd, T: transport::Transport, W: wire::Wire, const ID: u8> abi::Fiber<'d>
    for Connect<'scope, 'd, T, W, ID>
{
    type Output = io::Result<net::Io<'scope, 'd, W, connection::Id<'d, ID>>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, cx) = call.into_parts();
        let this = this.get_mut();
        let lease = match mem::replace(&mut this.stage, Stage::Done) {
            Stage::Done => return task::Poll::Pending,
            Stage::Ticket(lease) => lease,
            Stage::Init { addr, config } => match this.host.port.dial(addr, config) {
                Ok(lease) => lease,
                Err(error) => return task::Poll::Ready(Err(error)),
            },
        };
        let wake = cx.as_ref().completion_waker();
        this.poll_completion(lease, wake)
    }
}
