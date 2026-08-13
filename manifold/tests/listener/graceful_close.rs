use std::{cell::RefCell, convert::Infallible, io::Write, net::Shutdown, pin::Pin, rc::Rc};

use dope_core::io::recv::{Lease, View};
use dope_manifold::{
    Bundle, Outcome,
    listener::{connection, handler::Application},
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{
        self, ReadyOpen, RecvChunk, RuntimeLimits, Wire, reclaim,
        send::{Buffer, Plain, Prepared, Sent, Storage, Transition, Vectored},
    },
};
use dope_test::{fibers::Gate, scenario::scenarios::Listener};
use o3::buffer::bytes::{Borrowed, Bytes, Retainable};

const BYE: &[u8] = b"<<BYE>>";
const CONTROL: &[u8] = b"<<CONTROL>>";

struct Buffered;

impl Buffered {
    fn plain<'a, const CAP: usize>(
        mut send: Storage<'a, Buffer<CAP>>,
        plain: Plain<'_>,
    ) -> Prepared<'a, reclaim::OnSubmit> {
        let consumed = plain.len().min(send.spare_capacity());
        assert!(send.try_extend(&plain.as_slice()[..consumed]));
        send.buffered(consumed)
    }

    fn vectored<'a, const CAP: usize>(
        mut send: Storage<'a, Buffer<CAP>>,
        plain: Vectored<'_>,
    ) -> Prepared<'a, reclaim::OnSubmit> {
        let mut consumed = 0;
        for part in plain.iter() {
            let len = part.len().min(send.spare_capacity());
            assert!(send.try_extend(&part[..len]));
            consumed += len;
            if len != part.len() {
                break;
            }
        }
        send.buffered(consumed)
    }

    fn complete<'a, const CAP: usize>(
        mut send: Storage<'a, Buffer<CAP>>,
        sent: Sent,
    ) -> Transition<'a, reclaim::OnSubmit> {
        if !send.try_consume(sent.get()) {
            return Transition::unchanged(send.empty().close_after());
        }
        let prepared = if send.is_empty() {
            send.empty()
        } else {
            send.buffered(0)
        };
        Transition::unchanged(prepared)
    }
}

struct GracefulWire;

impl Wire for GracefulWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = ();
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::iter::Once<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type StorageBackend<'d>
        = Buffer<16384>
    where
        Self: 'd;
    type Reclaim = reclaim::OnSubmit;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(
        _: RuntimeLimits,
        _: Self::InitConfig<'d, ID>,
    ) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn holds_plain<'d, const ID: u8>(
        _: &Self::Connection<'d, ID>,
        send: &Self::StorageBackend<'d>,
    ) -> bool {
        !send.is_empty()
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(GracefulWire, Buffer::new())))
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
        send: Storage<'a, Buffer<16384>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Buffered::plain(send, plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<16384>>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Buffered::vectored(send, plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<16384>>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        Buffered::complete(send, sent)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<16384>>,
    ) -> Prepared<'a, Self::Reclaim> {
        if send.is_empty() {
            send.empty()
        } else {
            send.buffered(0)
        }
    }

    fn graceful_close<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        mut send: Storage<'a, Buffer<16384>>,
    ) -> Prepared<'a, Self::Reclaim> {
        assert!(send.try_extend(BYE));
        send.buffered(0)
    }
}

struct ProbeApp {
    payload: Option<Vec<u8>>,
    gate: Gate,
    chunk_gate: Option<Gate>,
}

impl<'d, const ID: u8> Application<'d, ID> for ProbeApp {
    type Conn = ();
    type Wire = GracefulWire;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, GracefulWire, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID> for ProbeApp {
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, GracefulWire, ()>,
        _chunk: R,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        if let Some(gate) = self.as_ref().get_ref().chunk_gate.as_ref() {
            gate.hit();
        }
        let Some(reply) = self.get_mut().payload.as_ref() else {
            return Outcome::Ok;
        };
        let n = reply.len();
        let mut write = connection.try_write().expect("listener write slot");
        write[..n].copy_from_slice(reply);
        write.submit(n, driver);
        Outcome::CloseAfter
    }
}

struct ControlWire {
    pending: bool,
}

impl Wire for ControlWire {
    type Connection<'d, const ID: u8> = Self;
    type ConnectionStorage<const ID: u8> = ();
    type InitConfig<'d, const ID: u8> = ();
    type RuntimeContext<'d, const ID: u8> = ();
    type Open<'a, 'd, const ID: u8>
        = ReadyOpen<Self::Connection<'d, ID>, Self::StorageBackend<'d>>
    where
        'd: 'a;
    type OpenError = Infallible;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::array::IntoIter<RecvChunk<'a, Self::Recv<'a>>, 2>;
    type RetainedRecv<'d> = View<'d>;
    type StorageBackend<'d>
        = Buffer<64>
    where
        Self: 'd;
    type Reclaim = reclaim::OnSubmit;
    type Receive = wire::receive::Direct;

    fn connection_storage<const ID: u8>(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d, const ID: u8>(
        _: RuntimeLimits,
        _: Self::InitConfig<'d, ID>,
    ) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn holds_plain<'d, const ID: u8>(
        _: &Self::Connection<'d, ID>,
        send: &Self::StorageBackend<'d>,
    ) -> bool {
        !send.is_empty()
    }

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(Self { pending: false }, Buffer::new())))
    }

    fn process_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: &'a mut [u8],
        _: &wire::batch::Capacity<Self>,
    ) -> Self::RecvBatch<'a>
    where
        'd: 'a,
    {
        wire.pending = true;
        let bytes = &*bytes;
        let (left, right) = bytes.split_at(bytes.len() / 2);
        [
            RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(left)),
            RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(right)),
        ]
        .into_iter()
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        wire.pending = true;
        Some(bytes.into_view())
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<64>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Buffered::plain(send, plain)
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<64>>,
        plain: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        Buffered::vectored(send, plain)
    }

    fn after_send<'a, 'd, const ID: u8>(
        _: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<64>>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        Buffered::complete(send, sent)
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Buffer<64>>,
    ) -> Prepared<'a, Self::Reclaim> {
        if std::mem::take(&mut wire.pending) {
            send.static_slice(CONTROL)
        } else {
            send.empty()
        }
    }
}

struct ControlApp {
    gate: Gate,
    received: Rc<RefCell<Vec<u8>>>,
}

impl<'d, const ID: u8> Application<'d, ID> for ControlApp {
    type Conn = ();
    type Wire = ControlWire;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn send(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, ControlWire, ()>,
        _sent: usize,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) {
        let _ = self;
        connection.set_close_after();
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, ControlWire, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for ControlApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, ControlWire, ()>,
        chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        self.get_mut()
            .received
            .borrow_mut()
            .extend_from_slice(chunk.as_ref());
        Outcome::Ok
    }
}

#[test]
fn graceful_sentinel_trails_drain_reply() {
    let want = dope_test::peer::Pattern::with_len(12_000).into_bytes();
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, GracefulWire, Throughput>, _>(
        ProbeApp {
            payload: Some(want.clone()),
            gate: gate.clone(),
            chunk_gate: None,
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"GET\n").expect("request");
                dope_test::peer::Peer::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            let mut expect = want;
            expect.extend_from_slice(BYE);
            assert_eq!(got, expect, "sentinel must trail the reply, before the FIN");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        },
    );
}

#[test]
fn graceful_sentinel_survives_peer_eof() {
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, GracefulWire, Throughput>, _>(
        ProbeApp {
            payload: None,
            gate: gate.clone(),
            chunk_gate: None,
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"REQ").expect("request");
                s.shutdown(Shutdown::Write).expect("half close");
                dope_test::peer::Peer::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(got, BYE, "peer EOF must not suppress the graceful sentinel");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        },
    );
}

#[test]
fn global_shutdown_quiesces_buffered_graceful_send() {
    let chunk = Gate::new();
    let close = Gate::new();
    let peer = Listener::new(64, Default::default())
        .run::<0, _, Bundle<Tcp, GracefulWire, Throughput>, _>(
            ProbeApp {
                payload: None,
                gate: close.clone(),
                chunk_gate: Some(chunk.clone()),
            },
            |case| {
                let peer = case.peer(|stream| {
                    stream.write_all(b"REQ").expect("request");
                    dope_test::peer::Peer::read_all(stream)
                });
                case.until(&chunk, 1);
                peer
            },
        );
    assert_eq!(peer.join().expect("peer join"), BYE);
    assert_eq!(close.hits(), 1);
}

#[test]
fn control_output_is_flushed_after_plaintext() {
    assert!(wire::batch::Capacity::<ControlWire>::fit(1).is_none());
    assert_eq!(
        wire::batch::Capacity::<ControlWire>::fit(2)
            .expect("pair capacity")
            .items()
            .get(),
        2
    );
    let gate = Gate::new();
    let received = Rc::new(RefCell::new(Vec::new()));
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, ControlWire, Throughput>, _>(
        ControlApp {
            gate: gate.clone(),
            received: received.clone(),
        },
        |case| {
            let peer = case.peer(|s| {
                s.write_all(b"REQ").expect("request");
                dope_test::peer::Peer::read_all(s)
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
        },
    );
}
