use std::convert;

use dope_core::io::recv;

use crate::wire::{self, batch};

/// Opaque receive-resource wait returned by a wire strategy.
pub struct Block<'a, 'd, const ID: u8, W: wire::Wire>(
    pub(crate) <<W as wire::Wire>::Receive as Strategy<W>>::Block<'a, 'd, ID>,
)
where
    W: 'd,
    'd: 'a;

/// A resource wait that already owns the runtime borrow used to register it.
pub trait Wait<'d, const ID: u8, W: wire::Wire>
where
    W: 'd,
{
    type Registration: Registration;

    fn register(self, credit: wire::RecvCredit<'d, ID>) -> Option<Self::Registration>;
}

/// A provisional resource wait committed only after its completion is stored.
pub trait Registration {
    fn commit(self);
}

/// A successful receive reservation. Implementations own every resource needed
/// to process one completion and return them from `Drop`.
pub trait Transaction<'d, W: wire::Wire> {
    fn process<'bytes>(
        &mut self,
        bytes: &'bytes mut [u8],
        capacity: &batch::Capacity<W>,
    ) -> W::RecvBatch<'bytes>
    where
        'd: 'bytes;

    fn process_retained<'bytes>(
        &mut self,
        bytes: recv::Lease<'bytes>,
    ) -> Option<W::RetainedRecv<'bytes>>
    where
        'd: 'bytes;
}

/// Static receive policy selected by a [`wire::Wire`] implementation.
pub trait Strategy<W: wire::Wire> {
    type Block<'a, 'd, const ID: u8>: Wait<'d, ID, W>
    where
        W: 'd,
        'd: 'a;
    type Transaction<'a, 'd, const ID: u8>: Transaction<'d, W>
    where
        W: 'd,
        'd: 'a;

    const BACKPRESSURE: bool;

    fn reserve<'a, 'd, const ID: u8>(
        wire: &'a mut W::Connection<'d, ID>,
        send: &'a mut W::StorageBackend<'d>,
        runtime: &'a mut W::RuntimeContext<'d, ID>,
    ) -> Result<Self::Transaction<'a, 'd, ID>, Self::Block<'a, 'd, ID>>
    where
        W: 'd,
        'd: 'a;

    /// Cancels an exact connection's registered resource wait, if any.
    fn cancel<'d, const ID: u8>(
        runtime: &mut W::RuntimeContext<'d, ID>,
        target: wire::RecvCreditId<'d, ID>,
    ) where
        W: 'd;

    /// Retries the oldest receive reservation after a retained wire resource
    /// has been returned.
    fn recv_released<'d, const ID: u8>(runtime: &mut W::RuntimeContext<'d, ID>)
    where
        W: 'd;

    /// Retries the oldest receive reservation after a send resource has been
    /// returned.
    fn send_released<'d, const ID: u8>(runtime: &mut W::RuntimeContext<'d, ID>)
    where
        W: 'd;
}

/// Zero-storage receive strategy for wires that never backpressure receive.
pub struct Direct;

pub struct DirectTransaction<'a, 'd, const ID: u8, W: wire::Wire> {
    wire: &'a mut W::Connection<'d, ID>,
    runtime: &'a mut W::RuntimeContext<'d, ID>,
}

impl<W: wire::Wire> Strategy<W> for Direct {
    type Block<'a, 'd, const ID: u8>
        = convert::Infallible
    where
        W: 'd,
        'd: 'a;
    type Transaction<'a, 'd, const ID: u8>
        = DirectTransaction<'a, 'd, ID, W>
    where
        W: 'd,
        'd: 'a;

    const BACKPRESSURE: bool = false;

    fn reserve<'a, 'd, const ID: u8>(
        wire: &'a mut W::Connection<'d, ID>,
        _: &'a mut W::StorageBackend<'d>,
        runtime: &'a mut W::RuntimeContext<'d, ID>,
    ) -> Result<Self::Transaction<'a, 'd, ID>, Self::Block<'a, 'd, ID>>
    where
        W: 'd,
        'd: 'a,
    {
        Ok(DirectTransaction { wire, runtime })
    }

    fn cancel<'d, const ID: u8>(_: &mut W::RuntimeContext<'d, ID>, _: wire::RecvCreditId<'d, ID>)
    where
        W: 'd,
    {
    }

    fn recv_released<'d, const ID: u8>(_: &mut W::RuntimeContext<'d, ID>)
    where
        W: 'd,
    {
    }

    fn send_released<'d, const ID: u8>(_: &mut W::RuntimeContext<'d, ID>)
    where
        W: 'd,
    {
    }
}

impl<'d, const ID: u8, W: wire::Wire + 'd> Wait<'d, ID, W> for convert::Infallible {
    type Registration = convert::Infallible;

    fn register(self, _: wire::RecvCredit<'d, ID>) -> Option<Self::Registration> {
        match self {}
    }
}

impl Registration for convert::Infallible {
    fn commit(self) {
        match self {}
    }
}

impl<'d, const ID: u8, W: wire::Wire> Transaction<'d, W> for DirectTransaction<'_, 'd, ID, W> {
    fn process<'bytes>(
        &mut self,
        bytes: &'bytes mut [u8],
        capacity: &batch::Capacity<W>,
    ) -> W::RecvBatch<'bytes>
    where
        'd: 'bytes,
    {
        let batch = W::process_recv(self.wire, self.runtime, bytes, capacity);
        debug_assert!(
            batch.len() <= capacity.items().get(),
            "wire receive batch exceeded its reserved capacity"
        );
        batch
    }

    fn process_retained<'bytes>(
        &mut self,
        bytes: recv::Lease<'bytes>,
    ) -> Option<W::RetainedRecv<'bytes>>
    where
        'd: 'bytes,
    {
        let retained = W::process_retained_recv(self.wire, self.runtime, bytes);
        debug_assert!(retained.as_ref().is_none_or(|cursor| {
            wire::Cursor::remaining(cursor) == 0 || !wire::Cursor::chunk(cursor).is_empty()
        }));
        retained
    }
}
