use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{ConnectOutcome, Connector};
use crate::fiber::{Holding, Io};
use crate::transport::Transport;
use crate::transport::wire::Wire;

enum Stage<T: Transport> {
    Init { addr: T::Addr, opts: T::StreamOpts },
    Ticket(u32),
}

pub struct Connect<'d, const ID: u8, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    host: Holding<'d, Connector<ID, T, W>>,
    stage: Stage<T>,
}

impl<'d, const ID: u8, T: Transport, W: Wire> Connect<'d, ID, T, W>
where
    T::Addr: Clone,
{
    pub(super) fn new(
        host: Holding<'d, Connector<ID, T, W>>,
        addr: T::Addr,
        opts: T::StreamOpts,
    ) -> Self {
        Self {
            host,
            stage: Stage::Init { addr, opts },
        }
    }
}

impl<const ID: u8, T: Transport, W: Wire> Unpin for Connect<'_, ID, T, W> where T::Addr: Clone {}

impl<'d, const ID: u8, T: Transport, W: Wire> Drop for Connect<'d, ID, T, W>
where
    T::Addr: Clone,
{
    fn drop(&mut self) {
        let Stage::Ticket(tag) = self.stage else {
            return;
        };
        let mut h = self.host.hold();
        h.as_mut().cancel_connect(tag);
    }
}

impl<'d, const ID: u8, T: Transport, W: Wire> Future for Connect<'d, ID, T, W>
where
    T::Addr: Clone,
{
    type Output = io::Result<Io<'d, Connector<ID, T, W>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let host_clone = this.host;
        let mut h = this.host.hold();
        let tag = match this.stage {
            Stage::Ticket(t) => t,
            Stage::Init { ref addr, opts } => {
                let addr = addr.clone();
                let Some(t) = h.as_mut().try_connect(addr, opts) else {
                    return Poll::Ready(Err(io::Error::other(
                        "fiber::Connector: pending pool exhausted",
                    )));
                };
                this.stage = Stage::Ticket(t);
                t
            }
        };
        match h.as_mut().try_resolve(tag, cx.waker()) {
            ConnectOutcome::Conn(token) => Poll::Ready(Ok(Io::new(host_clone, token))),
            ConnectOutcome::Failed(e) => Poll::Ready(Err(e)),
            ConnectOutcome::Pending => Poll::Pending,
        }
    }
}
