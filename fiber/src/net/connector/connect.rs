use std::io;
use std::pin::Pin;
use std::task::Poll;

use super::{ConnectOutcome, ConnectorHandle};
use crate::{Context, Fiber, Io};
use dope::manifold::connector::source::DialKey;
use dope_net::Transport;
use std::io::Error;

enum Stage<T: Transport> {
    Init {
        addr: T::Addr,
        config: T::StreamConfig,
    },
    Ticket(DialKey),
    Done,
}

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
    type Output = io::Result<Io<'d, ConnectorHandle<'scope, 'd, T>>>;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let key = match this.stage {
            Stage::Done => return Poll::Pending,
            Stage::Ticket(key) => key,
            Stage::Init { ref addr, config } => {
                let Some(key) = this.host.port.dial(addr.clone(), config) else {
                    return Poll::Ready(Err(Error::other(
                        "fiber::Connector: pending pool exhausted",
                    )));
                };
                this.stage = Stage::Ticket(key);
                key
            }
        };
        match this.host.port.resolve(key, unsafe { cx.waker_unchecked() }) {
            ConnectOutcome::Conn(token) => {
                this.stage = Stage::Done;
                Poll::Ready(Ok(Io::new(this.host, token)))
            }
            ConnectOutcome::Failed(error) => {
                this.stage = Stage::Done;
                Poll::Ready(Err(error))
            }
            ConnectOutcome::Pending => Poll::Pending,
        }
    }
}
