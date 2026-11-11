use crate::error;
use crate::state::buffers;
use crate::tls::{self, endpoint, receive::waiters, roles};
use dope::core::driver::{
    receive::credit,
    route::{self, table::conn},
};
use dope::net::wire::{receive::reserve, send, terminal};
use o3::depot::queue::tagged::entry;
use std::marker;

pub struct Blocked<'a, 'd, ID: route::Identity> {
    waiters: &'a mut waiters::Queue<'d, ID>,
    kind: waiters::Resource,
}
pub struct Policy<R>(marker::PhantomData<fn() -> R>);

/// Holds the admitted receive slot and any handshake output loan through processing.
pub struct Transaction<'a, 'w, 'd, R: roles::Protocol + 'w, ID: route::Identity + 'w>
where
    'd: 'w,
{
    wire: &'a mut tls::Connection<'w, R, ID>,
    send: Option<tls::Buffered<'a, 'w>>,
    run: &'a mut endpoint::Runtime<'w, 'd, R, ID>,
    recv: buffers::Reserve<'w>,
}

impl<'a, 'w, 'd, R: roles::Protocol + 'w, ID: route::Identity + 'w> Transaction<'a, 'w, 'd, R, ID>
where
    'd: 'w,
{
    pub(in crate::tls) fn into_parts(
        self,
    ) -> (
        &'a mut tls::Connection<'w, R, ID>,
        Option<tls::Buffered<'a, 'w>>,
        &'a mut endpoint::Runtime<'w, 'd, R, ID>,
        buffers::Reserve<'w>,
    ) {
        (self.wire, self.send, self.run, self.recv)
    }
}

pub(in crate::tls) fn release<'w, 'd: 'w, R: roles::Protocol + 'w, ID: route::Identity + 'w>(
    send: Option<tls::Buffered<'_, 'w>>,
    run: &mut endpoint::Runtime<'w, 'd, R, ID>,
    released: bool,
) {
    if released {
        run.waiters.wake(waiters::Resource::Recv);
    }
    if send.is_some_and(|send| send.complete() == send::Availability::Released) {
        run.waiters.wake(waiters::Resource::HandshakeEgress);
    }
}

impl<'w, 'd, R: roles::Protocol + 'w, ID: route::Identity + 'w> reserve::Completion
    for Transaction<'_, 'w, 'd, R, ID>
where
    'd: 'w,
{
    fn complete(self) {
        drop(self.recv);
        release(self.send, self.run, true);
    }
}
impl<R: roles::Protocol> reserve::Try<tls::Tls<R>> for Policy<R> {
    type Block<'a, 'w, 'd, ID: route::Identity + 'w>
        = Blocked<'a, 'd, ID>
    where
        tls::Tls<R>: 'w,
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: route::Identity + 'w>
        = Transaction<'a, 'w, 'd, R, ID>
    where
        tls::Tls<R>: 'w,
        'd: 'w,
        'w: 'a;

    /// Submitted ciphertext keeps its send owner until completion; receiving
    /// defers new native TLS output to after_send, preserving wire order.
    fn try_reserve<'a, 'w, 'd, ID: route::Identity + 'w>(
        wire: &'a mut tls::Connection<'w, R, ID>,
        send: Option<&'a mut tls::SendState<'w>>,
        run: &'a mut endpoint::Runtime<'w, 'd, R, ID>,
    ) -> reserve::Outcome<
        Self::Transaction<'a, 'w, 'd, ID>,
        Self::Block<'a, 'w, 'd, ID>,
        terminal::Error<'w, tls::Tls<R>>,
    >
    where
        tls::Tls<R>: 'w,
        'd: 'w,
        'w: 'a,
    {
        let recv = match wire.buffers.recv.try_reserve() {
            Ok(Some(recv)) => recv,
            Ok(None) => {
                return reserve::Outcome::Blocked(Blocked {
                    waiters: &mut run.waiters,
                    kind: waiters::Resource::Recv,
                });
            }
            Err(cause) => {
                wire.failed = true;
                return reserve::Outcome::Failed(terminal::Error::from_cause(
                    error::Error::Allocation(cause.into()),
                ));
            }
        };
        let send = if let (true, Some(send)) = (wire.native.is_handshaking(), send) {
            let Some(send) = send.reserve(&wire.buffers.send) else {
                drop(recv);
                run.waiters.wake(waiters::Resource::Recv);
                return reserve::Outcome::Blocked(Blocked {
                    waiters: &mut run.waiters,
                    kind: waiters::Resource::HandshakeEgress,
                });
            };
            Some(send)
        } else {
            None
        };
        reserve::Outcome::Ready(Transaction {
            wire,
            send,
            run,
            recv,
        })
    }
    fn cancel<'w, 'd, ID: route::Identity + 'w>(
        run: &mut endpoint::Runtime<'w, 'd, R, ID>,
        target: credit::Id<'d, ID>,
    ) where
        tls::Tls<R>: 'w,
        'd: 'w,
    {
        run.waiters.cancel(target);
    }
    fn release_recv<'w, 'd, ID: route::Identity + 'w>(run: &mut endpoint::Runtime<'w, 'd, R, ID>)
    where
        tls::Tls<R>: 'w,
        'd: 'w,
    {
        run.waiters.wake(waiters::Resource::Recv);
    }
    fn release_send<'w, 'd, ID: route::Identity + 'w>(run: &mut endpoint::Runtime<'w, 'd, R, ID>)
    where
        tls::Tls<R>: 'w,
        'd: 'w,
    {
        run.waiters.wake(waiters::Resource::HandshakeEgress);
    }
}
impl<'a, 'd, R: roles::Protocol, ID: route::Identity> reserve::Wait<'d, ID, tls::Tls<R>>
    for Blocked<'a, 'd, ID>
{
    type Registration = waiters::Registration<'a, 'd, ID>;
    fn register(
        self,
        credit: credit::Pending<'d, ID>,
    ) -> Result<Self::Registration, entry::Error<conn::Capacity>> {
        self.waiters.register(self.kind, credit)
    }
}
