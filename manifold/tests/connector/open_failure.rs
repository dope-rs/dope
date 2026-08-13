use std::{cell::Cell, convert::Infallible, error::Error, fmt, net::SocketAddr, rc::Rc};

use dope_core::{
    driver::{Context, settings},
    io::recv::{self, Lease, View},
};
use dope_manifold::{
    Bundle,
    connector::{
        app::{self, Application, ChunkOutcome},
        attempt::Id,
        connection,
    },
    timing::Throughput,
};
use dope_net::{
    link::egress::Queue,
    tcp::Tcp,
    wire::{
        self, Identity, ReadyOpen, RecvChunk, RuntimeLimits, Wire, reclaim,
        send::{Plain, Prepared, Sent, Storage, Transition, Vectored},
    },
};
use dope_test::{fibers::Gate, scenario::scenarios::AttemptConnector};
use o3::buffer::{
    bytes::{Borrowed, Bytes, Retainable},
    storage::Shared,
};

#[derive(Debug)]
struct TestOpenError;

impl fmt::Display for TestOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("permanent test open failure")
    }
}

impl Error for TestOpenError {}

struct FailingWire;

/// Returns temporary backpressure exactly once before allowing the connection
/// to open. This exercises the connector/runtime park boundary rather than
/// merely the dialer's local ready queue.
struct DeferredState {
    first: Cell<bool>,
    opens: Cell<usize>,
    deferred: Gate,
}

#[derive(Clone)]
pub(super) struct DeferredWire {
    state: Rc<DeferredState>,
}

impl DeferredWire {
    pub(super) fn opens(&self) -> usize {
        self.state.opens.get()
    }

    pub(super) fn deferred(&self) -> &Gate {
        &self.state.deferred
    }
}

impl Default for DeferredWire {
    fn default() -> Self {
        Self {
            state: Rc::new(DeferredState {
                first: Cell::new(true),
                opens: Cell::new(0),
                deferred: Gate::new(),
            }),
        }
    }
}

impl Wire for FailingWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = ();
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = TestOpenError;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type StorageBackend<'d>
        = ()
    where
        Self: 'd;
    type Reclaim = reclaim::OnComplete;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(_: RuntimeLimits, _: ()) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, TestOpenError>
    where
        'd: 'a,
    {
        Err(TestOpenError)
    }

    fn process_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        std::iter::once(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes)))
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        _: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        Some(bytes.into_view())
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::input(plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        _: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Prepared::vectored(plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        _: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        Transition::completed(send)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a, Self::Reclaim> {
        send.empty()
    }
}

impl Wire for DeferredWire {
    type Connection<'d, const ID: u8> = Identity;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = Self;
    type RuntimeContext<'d, const ID: u8> = Self;
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = recv::Shared<'d>;
    type StorageBackend<'d>
        = ()
    where
        Self: 'd;
    type Reclaim = reclaim::OnComplete;
    type Receive = wire::receive::Direct;
    const RAW_RECV: bool = true;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(_: RuntimeLimits, config: Self) -> std::io::Result<Self>
    where
        Self: 'd,
    {
        Ok(config)
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        runtime: &'a mut Self,
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        runtime.state.opens.set(runtime.state.opens.get() + 1);
        if runtime.state.first.replace(false) {
            runtime.state.deferred.hit();
            return Ok(None);
        }
        Ok(Some(ReadyOpen::new(Identity, ())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Self,
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        let capacity = wire::batch::Capacity::<Identity>::full();
        <Identity as Wire>::process_recv::<ID>(wire, &mut (), bytes, &capacity)
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut Self,
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        <Identity as Wire>::process_retained_recv::<ID>(wire, &mut (), bytes)
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::prepare_send::<ID>(wire, send, plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::prepare_send_vectored::<ID>(wire, send, plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        <Identity as Wire>::after_send::<ID>(wire, send, sent)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a, Self::Reclaim> {
        <Identity as Wire>::flush_pending::<ID>(wire, send)
    }
}

struct FailureApp {
    gate: Gate,
    failures: Rc<Cell<u32>>,
}

struct DeferredApp {
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for DeferredApp {
    type Conn = ();
    type Wire = DeferredWire;
    type Send = Shared;
    type Input = dope_manifold::receive::Borrowed;

    fn connection(&self) -> Self::Conn {}
}

impl<'d, const ID: u8> app::Receive<'d, ID> for DeferredApp {
    type Continuation = app::continuation::Complete;
}

impl<'d, const ID: u8> app::BorrowedReceive<'d, ID> for DeferredApp {
    fn chunk<O, R: Retainable>(
        &mut self,
        _: connection::Ctx<'_, 'd, ID, DeferredWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        _: R,
        _: &mut Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }
}

impl<'d, const ID: u8> app::Lifecycle<'d, ID> for DeferredApp {
    fn connected<O>(
        &mut self,
        _: Id<'d, ID>,
        _: dope_core::io::socket::Addr,
        _: connection::Ctx<'_, 'd, ID, DeferredWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        _: &mut Context<'_, 'd>,
    ) {
        self.gate.hit();
    }

    fn sent(&mut self, _: connection::Id<'d, ID>, _: bool) {}

    fn close<O>(
        &mut self,
        _: connection::Ctx<'_, 'd, ID, DeferredWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        reason: dope_manifold::connector::lifecycle::CloseReason,
        _: &mut Context<'_, 'd>,
    ) -> app::CloseOutcome {
        app::CloseOutcome::Complete(reason)
    }
}

impl<'d, const ID: u8> app::RequestSource<'d, ID> for DeferredApp {
    fn drain_requests(
        &self,
        _: connection::Id<'d, ID>,
        _: &mut Self::Conn,
        _: &mut app::RequestDrain<'_, 'd, Shared>,
        _: &mut Context<'_, 'd>,
    ) -> app::Requests {
        app::Requests::default()
    }
}

impl<'d, const ID: u8> app::Scheduling<'d, ID> for DeferredApp {
    fn pre_park<'turn>(
        &mut self,
        _: dope_core::driver::schedule::Application<'turn, 'd>,
        _: &mut o3::cell::region::Token<'d>,
    ) {
        let _ = self;
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(
        &self,
        _: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        dope_core::driver::schedule::Progress::Quiescent
    }
}

impl<'d, const ID: u8> Application<'d, ID> for FailureApp {
    type Conn = ();
    type Wire = FailingWire;
    type Send = Shared;
    type Input = dope_manifold::receive::Borrowed;

    fn connection(&self) -> Self::Conn {}
}

impl<'d, const ID: u8> app::Receive<'d, ID> for FailureApp {
    type Continuation = app::continuation::Complete;
}

impl<'d, const ID: u8> app::BorrowedReceive<'d, ID> for FailureApp {
    fn chunk<O, R: Retainable>(
        &mut self,
        _: connection::Ctx<'_, 'd, ID, FailingWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        _: R,
        _: &mut Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }
}

impl<'d, const ID: u8> app::Lifecycle<'d, ID> for FailureApp {
    fn connected<O>(
        &mut self,
        _: Id<'d, ID>,
        _: dope_core::io::socket::Addr,
        _: connection::Ctx<'_, 'd, ID, FailingWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        _: &mut Context<'_, 'd>,
    ) {
        panic!("a wire that failed to open must never connect");
    }

    fn open(
        &mut self,
        _: Id<'d, ID>,
        outcome: dope_manifold::connector::app::OpenOutcome<TestOpenError>,
        _: &mut Context<'_, 'd>,
    ) {
        let dope_manifold::connector::app::OpenOutcome::Failed(error) = outcome else {
            panic!("permanent wire failure must not be reported as deferred");
        };
        assert_eq!(
            error.to_string(),
            "wire open failed: permanent test open failure"
        );
        self.failures.set(self.failures.get() + 1);
        self.gate.hit();
    }

    fn sent(&mut self, _: connection::Id<'d, ID>, _: bool) {}

    fn close<O>(
        &mut self,
        _: connection::Ctx<'_, 'd, ID, FailingWire, (), O>,
        _: Queue<'_, 'd, 32, Shared>,
        reason: dope_manifold::connector::lifecycle::CloseReason,
        _: &mut Context<'_, 'd>,
    ) -> app::CloseOutcome {
        app::CloseOutcome::Complete(reason)
    }
}

impl<'d, const ID: u8> app::RequestSource<'d, ID> for FailureApp {
    fn drain_requests(
        &self,
        _: connection::Id<'d, ID>,
        _: &mut Self::Conn,
        _: &mut app::RequestDrain<'_, 'd, Shared>,
        _: &mut Context<'_, 'd>,
    ) -> app::Requests {
        app::Requests::default()
    }
}

impl<'d, const ID: u8> app::Scheduling<'d, ID> for FailureApp {
    fn pre_park<'turn>(
        &mut self,
        _: dope_core::driver::schedule::Application<'turn, 'd>,
        _: &mut o3::cell::region::Token<'d>,
    ) {
        let _ = self;
    }

    fn shutdown(&mut self) {
        let _ = self;
    }

    fn progress(
        &self,
        _: &o3::cell::region::Token<'d>,
    ) -> dope_core::driver::schedule::Progress<'d> {
        dope_core::driver::schedule::Progress::Quiescent
    }
}

#[test]
fn fatal_wire_open_is_reported_once_and_never_deferred() {
    let address: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let gate = Gate::new();
    let failures = Rc::new(Cell::new(0));

    AttemptConnector::new(address).run::<0, _, Bundle<Tcp, FailingWire, Throughput>, _>(
        FailureApp {
            gate: gate.clone(),
            failures: failures.clone(),
        },
        |case| {
            case.until(&gate, 1);
        },
    );

    assert_eq!(failures.get(), 1);
}

#[test]
fn temporary_wire_backpressure_retries_after_park() {
    let (address, server) = dope_test::peer::Peer::hold(1);
    let gate = Gate::new();

    AttemptConnector::new(address)
        .timer_cache_limit(settings::ScheduleCapacity::ZERO)
        .run::<1, _, Bundle<Tcp, DeferredWire, Throughput>, _>(
            DeferredApp { gate: gate.clone() },
            |case| {
                case.until(&gate, 1);
            },
        );

    server.join().expect("server join");
}
