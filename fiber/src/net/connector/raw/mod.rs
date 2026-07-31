use std::io;
use std::pin::Pin;
use std::task::Poll;

use super::ConnectorHandle;
use crate::io::Io;
use crate::raw::task::{CompletionOwner, CompletionRegistrar};
use crate::{Context, Fiber};
use dope::driver::ready::CompletionWaker;
use dope::manifold::connector::source::DialKey;
use dope_net::Transport;
use dope_net::wire::Wire;

enum Stage<T: Transport> {
    Init {
        addr: T::Addr,
        config: T::StreamConfig,
    },
    Ticket(DialKey),
    Done,
}

/// A connect operation that owns any waker registration made for its dial ticket.
pub struct Connect<'scope, 'd, T: Transport, W: Wire>
where
    T::Addr: Clone,
{
    host: ConnectorHandle<'scope, 'd, T, W>,
    stage: Stage<T>,
}

// SAFETY: Connect exclusively owns the dial ticket and cancels it in Drop.
// Ready paths retire the ticket before returning the connected handle.
unsafe impl<'scope, 'd, T, W> CompletionRegistrar<'d>
    for CompletionOwner<(&mut Connect<'scope, 'd, T, W>, DialKey)>
where
    T: Transport,
    T::Addr: Clone,
    W: Wire,
{
    type Output = Poll<io::Result<Io<'scope, 'd, W>>>;

    fn register(self, wake: CompletionWaker<'d>) -> Self::Output {
        let (owner, key) = self.0;
        owner.stage = Stage::Ticket(key);
        match owner.host.port.resolve(key, wake) {
            Poll::Ready(Ok(token)) => {
                owner.stage = Stage::Done;
                Poll::Ready(Ok(Io::new(&owner.host.port.connections, token)))
            }
            Poll::Ready(Err(error)) => {
                owner.stage = Stage::Done;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'scope, 'd, T: Transport, W: Wire> Connect<'scope, 'd, T, W>
where
    T::Addr: Clone,
{
    pub(super) fn new(
        host: ConnectorHandle<'scope, 'd, T, W>,
        addr: T::Addr,
        config: T::StreamConfig,
    ) -> Self {
        Self {
            host,
            stage: Stage::Init { addr, config },
        }
    }
}

impl<T: Transport, W: Wire> Unpin for Connect<'_, '_, T, W> where T::Addr: Clone {}

impl<T: Transport, W: Wire> Drop for Connect<'_, '_, T, W>
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

impl<'scope, 'd, T: Transport, W: Wire> Fiber<'d> for Connect<'scope, 'd, T, W>
where
    T::Addr: Clone,
{
    type Output = io::Result<Io<'scope, 'd, W>>;

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
        cx.as_ref()
            .register_completion(CompletionOwner((this, key)))
    }
}
