pub mod buffered;
pub mod identity;
pub mod reservation;
pub mod send;

use std::io;

use dope_core::driver::DriverRef;
use dope_core::driver::ready::ReadyKey;
use dope_core::driver::token::Token;
use o3::buffer::{Borrowed, Retained};

use self::send::{Plain, Prepared, SendStorage, Sent, Storage, Vectored};
pub use dope_core::io::recv::{Lease, View};

use crate::{Bytes, RetainBytes};

pub enum Reclaim {
    OnSubmit,
    OnComplete,
}

pub enum RecvChunk<'a, R> {
    /// A transformed range that aliases the current provided receive buffer.
    Borrowed(Bytes<Borrowed<'a>>),
    /// Wire-owned output independent of the current provided receive buffer.
    Owned(R),
}

/// A checked writer over uninitialized receive storage.
///
/// Its initialized length can only advance through methods that write every
/// byte in the committed prefix.
pub struct RecvTarget<'a> {
    buffer: &'a mut Vec<u8>,
    limit: usize,
}

impl<'a> RecvTarget<'a> {
    pub fn new(buffer: &'a mut Vec<u8>) -> Self {
        buffer.clear();
        let limit = buffer.capacity();
        Self { buffer, limit }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn remaining(&self) -> usize {
        self.limit - self.buffer.len()
    }

    pub fn write_prefix(&mut self, source: &[u8]) -> usize {
        let amount = source.len().min(self.remaining());
        self.buffer.extend_from_slice(&source[..amount]);
        amount
    }

    #[doc(hidden)]
    pub fn with_limit(&mut self, limit: usize, fill: impl FnOnce(&mut RecvTarget<'_>)) -> usize {
        let initial = self.buffer.len();
        let previous = self.limit;
        self.limit = initial + limit.min(self.remaining());
        fill(self);
        self.limit = previous;
        self.buffer.len() - initial
    }
}

/// An owned receive result that can be drained without allocation or
/// knowledge of its wire-specific representation.
pub trait RecvCursor: Unpin {
    fn remaining(&self) -> usize;

    /// Copies and consumes a logical prefix into checked uninitialized storage.
    fn read_into(&mut self, target: &mut RecvTarget<'_>);

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

impl RecvCursor for Bytes<Retained> {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn read_into(&mut self, target: &mut RecvTarget<'_>) {
        let count = target.write_prefix(self.as_slice());
        self.consume_prefix_up_to(count);
    }
}

impl RecvCursor for View<'_> {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn read_into(&mut self, target: &mut RecvTarget<'_>) {
        let count = target.write_prefix(self.as_slice());
        self.advance(count);
    }
}

/// A connection-local receive capability offered to a retained cursor.
///
/// A wire claims this capability only when the cursor owns a resource that
/// must be released before the same connection may receive again.
#[must_use]
pub struct RecvCredit<'d> {
    driver: DriverRef<'d>,
    ready: ReadyKey<'d>,
    target: Token,
}

/// A claimed receive capability. Dropping it schedules the exact connection
/// that supplied the capability, allowing its deferred receive stream to
/// resume.
#[must_use]
pub struct RecvCreditGuard<'d>(RecvCredit<'d>);

impl<'d> RecvCredit<'d> {
    pub(crate) fn new(driver: DriverRef<'d>, ready: ReadyKey<'d>, target: Token) -> Self {
        Self {
            driver,
            ready,
            target,
        }
    }

    pub fn claim(self) -> Result<RecvCreditGuard<'d>, Self> {
        if !self.driver.arm_recv_credit(self.ready, self.target) {
            return Err(self);
        }
        Ok(RecvCreditGuard(self))
    }
}

impl Drop for RecvCreditGuard<'_> {
    fn drop(&mut self) {
        self.0
            .driver
            .release_recv_credit(self.0.ready, self.0.target);
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeLimits {
    max_connections: usize,
    max_retained_recv_chunks: usize,
    max_recv_len: usize,
    recv_credit: bool,
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
            recv_credit: false,
        }
    }

    #[doc(hidden)]
    pub fn with_recv_credit(mut self) -> Self {
        self.recv_credit = true;
        self
    }

    pub fn max_connections(self) -> usize {
        self.max_connections
    }

    pub fn max_retained_recv_chunks(self) -> usize {
        self.max_retained_recv_chunks
    }

    pub fn max_recv_len(self) -> usize {
        self.max_recv_len
    }

    pub fn recv_credit(self) -> bool {
        self.recv_credit
    }
}

pub trait Wire: 'static + Sized {
    /// Connection-local state, scoped to the driver domain that owns it.
    type Connection<'d>: 'd
    where
        Self: 'd;
    /// Stable storage from which connection-local state may borrow.
    type ConnectionStorage: 'static;
    type InitConfig<'d>: 'd
    where
        Self: 'd;
    type RuntimeContext<'d>: 'd
    where
        Self: 'd;
    type Open<'a, 'd>: OpenReservation<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    /// A permanent failure while constructing connection-local wire state.
    ///
    /// Use [`std::convert::Infallible`] for wires whose open path cannot fail;
    /// it adds no storage to the `Result<Option<_>, _>` representation.
    type OpenError: std::error::Error + Send + Sync + 'static;
    type Recv<'a>: RetainBytes + 'a;
    type RecvBatch<'a>: ExactSizeIterator<Item = RecvChunk<'a, Self::Recv<'a>>>
    where
        Self: 'a;
    type RetainedRecv<'d>: RecvCursor + 'd
    where
        Self: 'd;
    type SendStorage: SendStorage;

    const RECLAIM: Reclaim;

    const RAW_RECV: bool = false;

    /// Whether this wire can retain a connection-local receive credit.
    const RECV_CREDIT: bool = false;

    fn connection_storage(capacity: usize) -> io::Result<Self::ConnectionStorage>;

    fn runtime_context<'d>(
        limits: RuntimeLimits,
        config: Self::InitConfig<'d>,
    ) -> io::Result<Self::RuntimeContext<'d>>
    where
        Self: 'd;

    /// Reserves wire state for one connection.
    ///
    /// `Ok(Some(_))` is ready to commit, `Ok(None)` is temporary backpressure,
    /// and `Err(_)` is a permanent open failure that must be surfaced instead
    /// of retried as capacity exhaustion.
    fn prepare_open<'a, 'd>(
        runtime: &'a mut Self::RuntimeContext<'d>,
    ) -> Result<Option<Self::Open<'a, 'd>>, Self::OpenError>
    where
        'd: 'a;

    fn holds_plain<'d>(_wire: &Self::Connection<'d>, _send: &Self::SendStorage) -> bool {
        false
    }

    /// Transforms one unique initialized provided-buffer completion. The wire
    /// may mutate `bytes` in place before returning borrowed ranges.
    fn process_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        runtime: &mut Self::RuntimeContext<'d>,
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a>;

    /// Transforms a receive completion whose result may outlive this call.
    /// Ownership of the provided buffer is explicit, so a wire may retain it
    /// directly or return an independent owned representation.
    fn process_retained_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        runtime: &mut Self::RuntimeContext<'d>,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>;

    /// Claims a connection-local receive credit for a retained cursor.
    ///
    /// The default leaves receive flow unchanged. A wire returns `Ok(())` only
    /// after `credit.claim()` succeeds and stores the resulting guard in
    /// `recv`. It releases that guard as soon as the resource imposing
    /// backpressure is released.
    fn bind_recv_credit<'d>(
        recv: &mut Self::RetainedRecv<'d>,
        credit: RecvCredit<'d>,
    ) -> Result<(), RecvCredit<'d>> {
        let _ = recv;
        Err(credit)
    }

    fn recv_eof<'d>(_wire: &mut Self::Connection<'d>) {}

    fn prepare_send<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a>;

    fn prepare_send_vectored<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        plain: Vectored<'a>,
    ) -> Prepared<'a>;

    fn submit_failed<'d>(_wire: &mut Self::Connection<'d>) {}

    fn after_send<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        sent: Sent,
    ) -> Prepared<'a>;

    fn flush_pending<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
    ) -> Prepared<'a>;

    fn graceful_close<'a, 'd>(
        _wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
    ) -> Prepared<'a> {
        send.empty(0)
    }
}

pub trait OpenReservation<W, S> {
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
