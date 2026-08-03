#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::RefCell;
use std::convert::Infallible;
use std::io::Write;
use std::net::Shutdown;
use std::pin::Pin;
use std::rc::Rc;

use dope::io::recv::{Lease, View};
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::egress::SlotEgress;
use dope::manifold::listener::state::EgressCtx;
use dope::manifold::{Outcome, listener};
use dope_net::link::slot::Slot;
use dope_net::wire::send::{Plain, Prepared, Sent, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RecvChunk, RuntimeLimits, Wire};
use dope_test::{Gate, Wired};
use o3::buffer::{Borrowed, Bytes, RetainBytes};

const BYE: &[u8] = b"<<BYE>>";
const CONTROL: &[u8] = b"<<CONTROL>>";

struct GracefulWire;

impl Wire for GracefulWire {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = ();
    type RuntimeContext<'d> = ();
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn connection_storage(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(_: RuntimeLimits, _: Self::InitConfig<'d>) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd>(_: &'a mut ()) -> Result<Option<Self::Open<'a, 'd>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(GracefulWire, ())))
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
        _send: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.len();
        Prepared::input(plain, consumed)
    }

    fn prepare_send_vectored<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
        _sent: Sent,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn graceful_close<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
    ) -> Prepared<'a> {
        Prepared::static_slice(BYE)
    }
}

struct ProbeApp {
    payload: Option<Vec<u8>>,
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for ProbeApp {
    type Conn = ();
    type Wire = GracefulWire;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, ProbeApp> for ProbeApp {
    fn chunk<R: RetainBytes>(
        app: Pin<&mut ProbeApp>,
        slot: &mut Slot<'d, GracefulWire, listener::state::State<()>>,
        mut egress: EgressCtx<'_, 'd, '_>,
        _chunk: R,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let Some(reply) = app.get_mut().payload.as_ref() else {
            return Outcome::Ok;
        };
        let n = reply.len();
        let mut buf = egress.write_buf_for(slot);
        buf[..n].copy_from_slice(reply);
        let ud = slot.token();
        slot.submit_buffered(buf, n, ud, driver);
        Outcome::CloseAfter
    }

    fn close(
        app: Pin<&mut ProbeApp>,
        _slot: &mut Slot<'d, GracefulWire, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
    ) {
        app.get_mut().gate.hit();
    }
}

struct ControlWire {
    pending: bool,
}

impl Wire for ControlWire {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = ();
    type RuntimeContext<'d> = ();
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::array::IntoIter<RecvChunk<'a, Self::Recv<'a>>, 2>;
    type RetainedRecv<'d> = View<'d>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn connection_storage(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(_: RuntimeLimits, _: Self::InitConfig<'d>) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd>(_: &'a mut ()) -> Result<Option<Self::Open<'a, 'd>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(Self { pending: false }, ())))
    }

    fn process_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        wire.pending = true;
        let bytes = &*bytes;
        let (left, right) = bytes.split_at(bytes.len() / 2);
        [
            RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(left)),
            RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(right)),
        ]
        .into_iter()
    }

    fn process_retained_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        wire.pending = true;
        let span = bytes.span(0, bytes.as_slice().len())?;
        bytes.into_view(span).ok()
    }

    fn prepare_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.len();
        Prepared::input(plain, consumed)
    }

    fn prepare_send_vectored<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a, 'd>(
        _: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
        _sent: Sent,
    ) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, ()>,
    ) -> Prepared<'a> {
        if std::mem::take(&mut wire.pending) {
            Prepared::static_slice(CONTROL)
        } else {
            send.empty(0)
        }
    }
}

struct ControlApp {
    gate: Rc<Gate>,
    received: Rc<RefCell<Vec<u8>>>,
}

impl<'d> Application<'d> for ControlApp {
    type Conn = ();
    type Wire = ControlWire;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, ControlApp> for ControlApp {
    fn chunk<R: RetainBytes>(
        app: Pin<&mut ControlApp>,
        _slot: &mut Slot<'d, ControlWire, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        chunk: R,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        app.get_mut()
            .received
            .borrow_mut()
            .extend_from_slice(chunk.as_slice());
        Outcome::Ok
    }

    fn send(
        _app: Pin<&mut ControlApp>,
        slot: &mut Slot<'d, ControlWire, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _sent: usize,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        slot.set_close_after();
    }

    fn close(
        app: Pin<&mut ControlApp>,
        _slot: &mut Slot<'d, ControlWire, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
    ) {
        app.get_mut().gate.hit();
    }
}

#[test]
fn graceful_sentinel_trails_drain_reply() {
    let want = dope_test::pattern(12_000);
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        transport: dope_net::tcp::listener::Config::default(),
        env: Wired<GracefulWire>,
        app: ProbeApp {
            payload: Some(want.clone()),
            gate: gate.clone(),
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"GET\n").expect("request");
                dope_test::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            let mut expect = want;
            expect.extend_from_slice(BYE);
            assert_eq!(got, expect, "sentinel must trail the reply, before the FIN");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        }
    }
}

#[test]
fn graceful_sentinel_survives_peer_eof() {
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        transport: dope_net::tcp::listener::Config::default(),
        env: Wired<GracefulWire>,
        app: ProbeApp {
            payload: None,
            gate: gate.clone(),
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"REQ").expect("request");
                s.shutdown(Shutdown::Write).expect("half close");
                dope_test::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(got, BYE, "peer EOF must not suppress the graceful sentinel");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        }
    }
}

#[test]
fn control_output_is_flushed_after_plaintext() {
    let gate = Gate::new();
    let received = Rc::new(RefCell::new(Vec::new()));
    dope_test::tcp_case! {
        max_connections: 64,
        transport: dope_net::tcp::listener::Config::default(),
        env: Wired<ControlWire>,
        app: ControlApp {
            gate: gate.clone(),
            received: received.clone(),
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"REQ").expect("request");
                dope_test::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(got, CONTROL);
            assert_eq!(
                received.borrow().as_slice(),
                b"REQ",
                "every wire receive chunk must reach the application in order"
            );
            assert_eq!(gate.hits(), 1);
        }
    }
}
