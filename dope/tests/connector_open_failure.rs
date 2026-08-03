#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::Cell;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use dope::DriverContext;
use dope::io::recv::{Lease, View};
use dope::manifold::connector::app::{ChunkOutcome, ConnApp};
use dope::manifold::connector::source::DialKey;
use dope::manifold::connector::state::State;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Throughput;
use dope_net::link::egress::queue::Queue;
use dope_net::link::raw::pool::outbound::OpenFailure;
use dope_net::link::slot::Slot;
use dope_net::tcp::Tcp;
use dope_net::wire::send::{Plain, Prepared, Sent, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RecvChunk, RuntimeLimits, Wire};
use dope_test::Gate;
use o3::buffer::{Borrowed, Bytes, RetainBytes, Shared};

#[derive(Debug)]
struct TestOpenError;

impl fmt::Display for TestOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("permanent test open failure")
    }
}

impl Error for TestOpenError {}

struct FailingWire;

impl Wire for FailingWire {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = ();
    type RuntimeContext<'d> = ();
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type OpenError = TestOpenError;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn connection_storage(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(_: RuntimeLimits, _: ()) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd>(_: &'a mut ()) -> Result<Option<Self::Open<'a, 'd>>, TestOpenError>
    where
        'd: 'a,
    {
        Err(TestOpenError)
    }

    fn process_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        std::iter::once(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes)))
    }

    fn process_retained_recv<'a, 'd>(
        _: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        let span = bytes.span(0, bytes.as_slice().len())?;
        bytes.into_view(span).ok()
    }

    fn prepare_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let len = plain.len();
        Prepared::input(plain, len)
    }

    fn prepare_send_vectored<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let len = plain.bytes();
        Prepared::vectored(plain, len)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
        _: Sent,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a> {
        send.empty(0)
    }
}

struct FailureApp {
    gate: Rc<Gate>,
    failures: Rc<Cell<u32>>,
}

impl<'d> ConnApp<'d> for FailureApp {
    type Conn = ();
    type Wire = FailingWire;
    type Send = Shared;

    fn chunk<R: RetainBytes>(
        &mut self,
        _: &mut Slot<'d, FailingWire, State<(), Shared>>,
        _: Queue<'_, 'd, '_, 32, Shared>,
        _: R,
        _: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }

    fn connected(
        &mut self,
        _: DialKey,
        _: &mut Slot<'d, FailingWire, State<(), Shared>>,
        _: Queue<'_, 'd, '_, 32, Shared>,
        _: &mut DriverContext<'_, 'd>,
    ) {
        panic!("a wire that failed to open must never connect");
    }

    fn open_failed(
        &mut self,
        _: DialKey,
        error: OpenFailure<TestOpenError>,
        _: &mut DriverContext<'_, '_>,
    ) {
        assert_eq!(
            error.to_string(),
            "wire open failed: permanent test open failure"
        );
        self.failures.set(self.failures.get() + 1);
        self.gate.hit();
    }

    fn send(
        &mut self,
        _: &mut Slot<'d, FailingWire, State<(), Shared>>,
        _: Queue<'_, 'd, '_, 32, Shared>,
        _: usize,
        _: &mut DriverContext<'_, 'd>,
    ) {
    }

    fn close(
        &mut self,
        _: &mut Slot<'d, FailingWire, State<(), Shared>>,
        _: Queue<'_, 'd, '_, 32, Shared>,
        _: &mut DriverContext<'_, 'd>,
    ) {
    }
}

#[test]
fn fatal_wire_open_is_reported_once_and_never_deferred() {
    let address: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let gate = Gate::new();
    let failures = Rc::new(Cell::new(0));

    dope_test::connector_case! {
        id: 0,
        max_connections: 1,
        address: address,
        backoff: Duration::from_millis(1),
        env: Bundle<Tcp, FailingWire, Throughput>,
        app: FailureApp {
            gate: gate.clone(),
            failures: failures.clone(),
        },
        |case| {
            case.until(&gate, 1);
        }
    }

    assert_eq!(failures.get(), 1);
}
