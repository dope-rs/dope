use std::{cell::RefCell, convert::Infallible, io::Write, pin::Pin, rc::Rc};

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

const HANDSHAKE: &[u8] = b"hello";
const WIRE_PREFIX: u8 = 0xa5;

const ROUNDS: u8 = 2;

const FRAMES: &[&[u8]] = &[b"PREAMBLE", b"SETTINGS"];

struct DeferredWire {
    rounds: u8,
    pending: Vec<u8>,
}

impl DeferredWire {
    fn established(&self) -> bool {
        self.rounds >= ROUNDS
    }

    fn prepare<'a>(
        &mut self,
        mut send: Storage<'a, Buffer<1024>>,
        consumed: usize,
    ) -> Prepared<'a, reclaim::OnSubmit> {
        if self.established() && send.is_empty() && !self.pending.is_empty() {
            assert!(send.try_extend(&[WIRE_PREFIX]));
            assert!(send.try_extend(&self.pending));
            self.pending.clear();
        }
        if send.is_empty() {
            send.consume(consumed)
        } else {
            send.buffered(consumed)
        }
    }
}

impl Wire for DeferredWire {
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
    type RecvBatch<'a> = std::option::IntoIter<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = View<'d>;
    type StorageBackend<'d>
        = Buffer<1024>
    where
        Self: 'd;
    type Reclaim = reclaim::OnSubmit;
    type Receive = wire::receive::Direct;

    const RAW_RECV: bool = true;

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

    fn prepare_open<'a, 'd, const ID: u8>(
        _: &'a mut (),
    ) -> Result<Option<Self::Open<'a, 'd, ID>>, Infallible>
    where
        'd: 'a,
    {
        Ok(Some(ReadyOpen::new(
            Self {
                rounds: 0,
                pending: Vec::new(),
            },
            Buffer::new(),
        )))
    }

    fn holds_plain<'d, const ID: u8>(
        wire: &Self::Connection<'d, ID>,
        send: &Self::StorageBackend<'d>,
    ) -> bool {
        !wire.pending.is_empty() || !send.is_empty()
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
        if !wire.established() {
            wire.rounds += 1;
            return None.into_iter();
        }
        Some(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes))).into_iter()
    }

    fn process_retained_recv<'a, 'd, const ID: u8>(
        wire: &mut Self::Connection<'d, ID>,
        _: &mut (),
        bytes: Lease<'a>,
    ) -> Option<Self::RetainedRecv<'a>>
    where
        'd: 'a,
    {
        if !wire.established() {
            wire.rounds += 1;
            return None;
        }
        Some(bytes.into_view())
    }

    fn prepare_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        wire.pending.extend_from_slice(plain.as_slice());
        wire.prepare(send, plain.len())
    }

    fn prepare_send_vectored<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
        vectored: Vectored<'a>,
    ) -> Prepared<'a, Self::Reclaim> {
        let mut consumed = 0;
        for plain in vectored.iter() {
            if plain.is_empty() {
                continue;
            }
            wire.pending.extend_from_slice(plain);
            consumed += plain.len();
        }
        wire.prepare(send, consumed)
    }

    fn after_send<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        mut send: Storage<'a, Self::StorageBackend<'d>>,
        sent: Sent,
    ) -> Transition<'a, Self::Reclaim> {
        assert!(send.try_consume(sent.get()));
        Transition::unchanged(wire.prepare(send, 0))
    }

    fn flush_pending<'a, 'd, const ID: u8>(
        wire: &'a mut Self::Connection<'d, ID>,
        send: Storage<'a, Self::StorageBackend<'d>>,
    ) -> Prepared<'a, Self::Reclaim> {
        wire.prepare(send, 0)
    }
}

struct PreambleApp {
    frames: &'static [&'static [u8]],
    sends: Rc<RefCell<Vec<usize>>>,
    gate: Gate,
}

impl PreambleApp {
    fn want_bytes(&self) -> usize {
        self.frames.iter().map(|f| f.len()).sum()
    }
}

impl<'d, const ID: u8> Application<'d, ID> for PreambleApp {
    type Conn = ();
    type Wire = DeferredWire;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, DeferredWire, ()>,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        for frame in self.get_mut().frames {
            let mut write = connection.try_write().expect("listener write slot");
            write[..frame.len()].copy_from_slice(frame);
            write.submit(frame.len(), driver);
        }
        Outcome::Ok
    }

    fn send(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, DeferredWire, ()>,
        sent: usize,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) {
        let this = self.get_mut();
        this.sends.borrow_mut().push(sent);
        if this.sends.borrow().iter().sum::<usize>() >= this.want_bytes() {
            connection.set_close_after();
        }
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, DeferredWire, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for PreambleApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, DeferredWire, ()>,
        _chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

#[test]
fn frames_written_before_wire_established_are_each_reported() {
    let gate = Gate::new();
    let sends = Rc::new(RefCell::new(Vec::new()));
    Listener::new(16, Default::default()).run::<0, _, Bundle<Tcp, DeferredWire, Throughput>, _>(
        PreambleApp {
            frames: FRAMES,
            sends: sends.clone(),
            gate: gate.clone(),
        },
        |case| {
            let peer = case.peer(|s| {
                for _ in 0..ROUNDS {
                    s.write_all(HANDSHAKE).expect("handshake");
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                dope_test::peer::Peer::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            let want: Vec<u8> = FRAMES
                .iter()
                .flat_map(|frame| std::iter::once(WIRE_PREFIX).chain(frame.iter().copied()))
                .collect();
            assert_eq!(
                got, want,
                "wire-expanded frames must reach the peer once and in order; a repeat means the \
                 slot re-handed plaintext the wire had already consumed"
            );
            let lens: Vec<usize> = FRAMES.iter().map(|f| f.len()).collect();
            assert_eq!(
                *sends.borrow(),
                lens,
                "callbacks must report consumed plaintext, never expanded wire bytes"
            );
        },
    );
}
