pub mod batch;
pub(crate) mod contract;
pub mod pools;
pub mod receive;
pub mod reclaim;
pub mod reservation;
pub mod send;
mod types;

use std::{error, io, mem, ops};

use dope_core::{
    driver::{self, route, schedule::ready},
    io::recv,
};
use o3::buffer::{bytes, resident};
pub use types::{identity::Identity, retained::RetainedBytes};

pub(crate) const MAX_RECV_BATCH_LIMIT: usize = 32;

pub enum RecvChunk<'a, R> {
    /// A transformed range that aliases the current provided receive buffer.
    Borrowed(bytes::Bytes<bytes::Borrowed<'a>>),
    /// Wire-owned output independent of the current provided receive buffer.
    Owned(R),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainError {
    Range,
    Capacity,
}

/// An owned logical byte cursor exposed as one stable contiguous chunk at a time.
/// A non-empty chunk cannot exceed `remaining`; `consume(n)` removes exactly
/// its result from that chunk, and invalid retention returns `RetainError::Range`.
pub trait Cursor<'d>: Unpin {
    /// Returns the current contiguous logical chunk.
    /// A non-empty cursor must return a non-empty chunk.
    fn chunk(&self) -> &[u8];

    /// Consumes at most the current chunk and returns the amount consumed.
    fn consume(&mut self, requested: usize) -> usize;

    fn remaining(&self) -> usize;

    fn retain(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<RetainedBytes<'d>, RetainError>;

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

/// Connection-local receive capability for a cursor retaining a blocked resource.
#[must_use]
pub struct RecvCredit<'d, const ID: u8> {
    ready: ready::Handle<'d>,
    target: route::Operation<'d, route::KeyTag<ID>>,
}

/// A claimed receive capability. Dropping it schedules the exact connection
/// that supplied the capability, allowing its deferred receive stream to
/// resume.
#[must_use]
pub struct RecvCreditGuard<'d, const ID: u8>(RecvCredit<'d, ID>);

/// Route-erased resource-return guard. This exposes no connection operation or
/// identity; it exists only for retained cursors whose associated type is
/// shared by all routes.
#[doc(hidden)]
#[must_use]
pub struct ErasedRecvCreditGuard<'d> {
    ready: ready::Handle<'d>,
    target: route::Erased<'d>,
}

/// Opaque identity for one route- and driver-branded receive credit.
///
/// A strategy cannot cancel a credit issued by a different route:
///
/// ```compile_fail
/// use dope_net::wire::RecvCreditId;
///
/// fn cross_route<'d>(credit: RecvCreditId<'d, 1>) -> RecvCreditId<'d, 2> {
///     credit
/// }
/// ```
///
/// The driver brand cannot escape its owning domain:
///
/// ```compile_fail
/// use dope_net::wire::RecvCreditId;
///
/// fn escape<'d>(credit: RecvCreditId<'d, 1>) -> RecvCreditId<'static, 1> {
///     credit
/// }
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RecvCreditId<'d, const ID: u8> {
    target: route::Operation<'d, route::KeyTag<ID>>,
}

/// Proof that an exact receive credit was claimed and converted into a stored
/// route-erased guard.
#[doc(hidden)]
pub struct RecvCreditReceipt<'d, const ID: u8> {
    ready: ready::FixedIdentity<'d>,
    target: route::Operation<'d, route::KeyTag<ID>>,
}

#[derive(Clone, Copy)]
pub(crate) struct RecvCreditBinding<'d, const ID: u8> {
    ready: ready::FixedIdentity<'d>,
    target: route::Operation<'d, route::KeyTag<ID>>,
}

impl<'d, const ID: u8> RecvCreditReceipt<'d, ID> {
    pub(crate) fn binds(&self, binding: RecvCreditBinding<'d, ID>) -> bool {
        self.ready == binding.ready && self.target == binding.target
    }
}

const _: () = assert!(
    mem::size_of::<RecvCreditBinding<'static, 0>>()
        == mem::size_of::<(
            ready::FixedIdentity<'static>,
            route::Operation<'static, route::KeyTag<0>>,
        )>()
);
const _: () = assert!(
    mem::size_of::<RecvCreditGuard<'static, 0>>()
        == mem::size_of::<(
            ready::Handle<'static>,
            route::Operation<'static, route::KeyTag<0>>,
        )>()
);

impl<'d, const ID: u8> RecvCredit<'d, ID> {
    pub(crate) fn new(
        ready: ready::Handle<'d>,
        target: route::Operation<'d, route::KeyTag<ID>>,
    ) -> Self {
        Self { ready, target }
    }

    pub fn claim(self) -> Result<RecvCreditGuard<'d, ID>, Self> {
        if !self.ready.arm_recv_credit(self.target) {
            return Err(self);
        }
        Ok(RecvCreditGuard(self))
    }

    pub fn id(&self) -> RecvCreditId<'d, ID> {
        RecvCreditId::new(self.target)
    }

    pub(crate) fn binding(&self) -> RecvCreditBinding<'d, ID> {
        RecvCreditBinding {
            ready: self.ready.identity(),
            target: self.target,
        }
    }
}

impl<'d, const ID: u8> RecvCreditGuard<'d, ID> {
    pub fn id(&self) -> RecvCreditId<'d, ID> {
        self.0.id()
    }

    #[doc(hidden)]
    pub fn erase(self) -> (ErasedRecvCreditGuard<'d>, RecvCreditReceipt<'d, ID>) {
        let this = mem::ManuallyDrop::new(self);
        (
            ErasedRecvCreditGuard {
                ready: this.0.ready,
                target: this.0.target.erase(),
            },
            RecvCreditReceipt {
                ready: this.0.ready.identity(),
                target: this.0.target,
            },
        )
    }

    /// Disarms this exact credit without scheduling its connection.
    #[doc(hidden)]
    pub fn cancel(self) {
        self.0.ready.cancel_recv_credit(self.0.target);
        mem::forget(self);
    }

    /// Schedules this exact connection to retry without announcing that a
    /// retained receive resource was returned.
    #[doc(hidden)]
    pub fn retry(self) {
        self.0
            .ready
            .wake_recv_credit(self.0.target, driver::RecvCreditWake::WaiterRetry);
        mem::forget(self);
    }
}

impl<const ID: u8> Drop for RecvCreditGuard<'_, ID> {
    fn drop(&mut self) {
        self.0
            .ready
            .release_recv_credit(self.0.target, driver::RecvCreditWake::ResourceReturned);
    }
}

impl<const ID: u8> Clone for RecvCreditGuard<'_, ID> {
    fn clone(&self) -> Self {
        assert!(
            self.0.ready.retain_recv_credit(self.0.target),
            "dope: inactive receive credit cloned"
        );
        Self(RecvCredit {
            ready: self.0.ready,
            target: self.0.target,
        })
    }
}

impl ErasedRecvCreditGuard<'_> {
    /// Disarms this exact credit without scheduling its connection.
    #[doc(hidden)]
    pub fn cancel(self) {
        self.ready.cancel_recv_credit(self.target);
        mem::forget(self);
    }

    /// Schedules this exact connection for waiter retry.
    #[doc(hidden)]
    pub fn retry(self) {
        self.ready
            .wake_recv_credit(self.target, driver::RecvCreditWake::WaiterRetry);
        mem::forget(self);
    }
}

impl Drop for ErasedRecvCreditGuard<'_> {
    fn drop(&mut self) {
        self.ready
            .release_recv_credit(self.target, driver::RecvCreditWake::ResourceReturned);
    }
}

impl Clone for ErasedRecvCreditGuard<'_> {
    fn clone(&self) -> Self {
        assert!(
            self.ready.retain_recv_credit(self.target),
            "dope: inactive receive credit cloned"
        );
        Self {
            ready: self.ready,
            target: self.target,
        }
    }
}

impl<'d, const ID: u8> RecvCreditId<'d, ID> {
    pub fn index(self) -> usize {
        self.target.slot().raw() as usize
    }

    pub(crate) const fn new(target: route::Operation<'d, route::KeyTag<ID>>) -> Self {
        Self { target }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeLimits {
    max_connections: usize,
    max_retained_recv_chunks: usize,
    max_recv_len: usize,
}

impl RuntimeLimits {
    pub fn new(
        max_connections: usize,
        max_retained_recv_chunks: usize,
        max_recv_len: usize,
    ) -> Self {
        Self {
            max_connections,
            max_retained_recv_chunks,
            max_recv_len,
        }
    }

    pub fn max_connections(self) -> usize {
        self.max_connections
    }

    /// Combined application and completed-batch retention budget.
    pub fn max_retained_recv_chunks(self) -> usize {
        self.max_retained_recv_chunks
    }

    pub fn max_recv_len(self) -> usize {
        self.max_recv_len
    }
}

pub trait Wire: 'static + Sized {
    /// Connection-local state, scoped to the driver domain that owns it.
    type Connection<'d, const ID: u8>: 'd
    where
        Self: 'd;
    /// Stable storage from which connection-local state may borrow.
    type ConnectionStorage<const ID: u8>: 'static;
    type InitConfig<'d, const ID: u8>: 'd
    where
        Self: 'd;
    /// Runtime state branded by the route that owns every call using it.
    type RuntimeContext<'d, const ID: u8>: 'd
    where
        Self: 'd;
    type Open<'a, 'd, const ID: u8>: OpenReservation<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    /// Permanent open failure; infallible wires use [`std::convert::Infallible`].
    type OpenError: error::Error + Send + Sync + 'static;
    type Recv<'a>: bytes::Retainable + 'a;
    type RecvBatch<'a>: batch::raw::Source<Item = RecvChunk<'a, Self::Recv<'a>>>
    where
        Self: 'a;
    type RetainedRecv<'d>: Cursor<'d> + 'd
    where
        Self: 'd;
    type StorageBackend<'d>: send::StorageBackend + 'd
    where
        Self: 'd;
    type Reclaim: reclaim::Policy;
    type Receive: receive::Strategy<Self>;

    const RAW_RECV: bool = false;

    /// Whether this wire can retain a connection-local receive credit.
    const RECV_CREDIT: bool = false;

    fn connection_storage<const ID: u8>(capacity: usize)
    -> io::Result<Self::ConnectionStorage<ID>>;

    fn runtime_context<'d, const ID: u8>(
        limits: RuntimeLimits,
        config: Self::InitConfig<'d, ID>,
    ) -> io::Result<Self::RuntimeContext<'d, ID>>
    where
        Self: 'd;

    /// Reserves wire state: `None` is backpressure and `Err` is permanent failure.
    fn prepare_open<'a, 'd, const ID: u8>(
        runtime: &'a mut Self::RuntimeContext<'d, ID>,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Self::OpenError>
    where
        'd: 'a;

    fn holds_plain<'d, const ID: u8>(
        _wire: &Self::Connection<'d, ID>,
        _send: &Self::StorageBackend<'d>,
    ) -> bool {
        false
    }

    /// Transforms one unique initialized provided-buffer completion. The wire
    /// may mutate `bytes` in place before returning borrowed ranges.
    /// The returned batch length must not exceed `capacity`.
    fn process_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        runtime: &mut Self::RuntimeContext<'d, ID>,
        bytes: &'a mut [u8],
        capacity: &batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a;

    /// Transforms a receive completion whose result may outlive this call.
    /// Ownership of the provided buffer is explicit, so a wire may retain it
    /// directly or return an independent owned representation.
    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        runtime: &mut Self::RuntimeContext<'d, ID>,
        bytes: recv::Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a;

    /// Claims receive credit only after retaining the resulting guard in `recv`.
    /// The retained cursor must release the resource that caused backpressure
    /// before it drops the guard, so the resumed strategy observes capacity.
    fn bind_recv_credit<'d, const ID: u8>(
        recv: &mut Self::RetainedRecv<'d>,
        credit: RecvCredit<'d, ID>,
    ) -> Result<RecvCreditReceipt<'d, ID>, RecvCredit<'d, ID>> {
        let _ = recv;
        Err(credit)
    }

    fn recv_eof<'d, const ID: u8>(_wire: &mut Self::Connection<'d, ID>) {}

    fn prepare_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, Self::StorageBackend<'d>>,
        plain: send::Plain<'a>,
    ) -> send::Prepared<'a, Self::Reclaim>;

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, Self::StorageBackend<'d>>,
        plain: send::Vectored<'a>,
    ) -> send::Prepared<'a, Self::Reclaim>;

    fn submit_failed<'d, const ID: u8>(_wire: &mut Self::Connection<'d, ID>) {}

    /// Consumes a send completion and reports returned shared storage.
    /// `OnComplete` must complete; only `OnSubmit` may prepare independent
    /// follow-up output.
    fn after_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, Self::StorageBackend<'d>>,
        sent: send::Sent,
    ) -> send::Transition<'a, Self::Reclaim>;

    fn flush_pending<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, Self::StorageBackend<'d>>,
    ) -> send::Prepared<'a, Self::Reclaim>;

    fn graceful_close<'a, 'd, const ID: u8>(
        _wire: &'a mut Self::Connection<'d, ID>,
        send: send::Storage<'a, Self::StorageBackend<'d>>,
    ) -> send::Prepared<'a, Self::Reclaim> {
        send.empty()
    }
}

/// A library-owned transaction which yields connection wire state exactly once.
///
/// The two supported forms cover immediately ready state and rollback into an
/// owning runtime. Foreign commit code cannot enter the post-submission commit
/// boundary:
///
/// ```compile_fail,E0277
/// use dope_net::wire::OpenReservation;
///
/// struct Foreign;
/// impl OpenReservation<(), ()> for Foreign {
///     fn commit(self) -> ((), ()) { ((), ()) }
/// }
/// ```
pub trait OpenReservation<W, S>: contract::Contract {
    fn commit(self) -> (W, S);
}

pub trait OpenRollback<W, S> {
    fn rollback_open(&mut self, open: (W, S));
}

pub struct ReadyOpen<W, S> {
    committed: (W, S),
}

impl<W, S> ReadyOpen<W, S> {
    pub fn new(wire: W, send: S) -> Self {
        Self {
            committed: (wire, send),
        }
    }
}

impl<W, S> OpenReservation<W, S> for ReadyOpen<W, S> {
    fn commit(self) -> (W, S) {
        self.committed
    }
}

impl<W, S> contract::Contract for ReadyOpen<W, S> {}
