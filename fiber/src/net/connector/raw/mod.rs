use std::io;
use std::pin::Pin;
use std::task::Poll;

use super::ConnectorHandle;
use crate::io::Io;
use crate::{Context, Fiber};
use dope::manifold::connector::source::DialKey;
use dope_net::Transport;

enum Stage<T: Transport> {
    Init {
        addr: T::Addr,
        config: T::StreamConfig,
    },
    Ticket(DialKey),
    Done,
}

/// A connect operation that owns any waker registration made for its dial ticket.
pub struct Connect<'scope, 'd, T: Transport>
where
    T::Addr: Clone,
{
    host: ConnectorHandle<'scope, 'd, T>,
    stage: Stage<T>,
}

impl<'scope, 'd, T: Transport> Connect<'scope, 'd, T>
where
    T::Addr: Clone,
{
    pub(super) fn new(
        host: ConnectorHandle<'scope, 'd, T>,
        addr: T::Addr,
        config: T::StreamConfig,
    ) -> Self {
        Self {
            host,
            stage: Stage::Init { addr, config },
        }
    }
}

impl<T: Transport> Unpin for Connect<'_, '_, T> where T::Addr: Clone {}

impl<T: Transport> Drop for Connect<'_, '_, T>
where
    T::Addr: Clone,
{
    fn drop(&mut self) {
        let Stage::Ticket(key) = self.stage else {
            return;
        };
        self.host.port.cancel(key);
    }
}

impl<'scope, 'd, T: Transport> Fiber<'d> for Connect<'scope, 'd, T>
where
    T::Addr: Clone,
{
    type Output = io::Result<Io<'scope, 'd>>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let key = match this.stage {
            Stage::Done => return Poll::Pending,
            Stage::Ticket(key) => key,
            Stage::Init { ref addr, config } => {
                let key = match this.host.port.dial(addr.clone(), config) {
                    Ok(key) => key,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                this.stage = Stage::Ticket(key);
                key
            }
        };
        // SAFETY: fields are private, so a live ticket can only be owned by
        // this `Connect`. Its Drop cancels that exact ticket and removes the
        // copied waker before the task context backing it can disappear.
        let waker = unsafe { cx.waker_unchecked() };
        match this.host.port.resolve(key, waker) {
            Poll::Ready(Ok(token)) => {
                this.stage = Stage::Done;
                Poll::Ready(Ok(Io::new(&this.host.port.connections, token)))
            }
            Poll::Ready(Err(error)) => {
                this.stage = Stage::Done;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
