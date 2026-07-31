#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::RefCell;
use std::io::Write;
use std::pin::Pin;
use std::rc::Rc;

use dope::io::provided::{ProvidedLease, ProvidedView};
use dope::manifold::Outcome;
use dope::manifold::listener;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::egress::SlotEgress;
use dope_net::link::slot::Slot;
use dope_net::wire::send::{Plain, Prepared, SendBuf, Sent, Storage, Vectored};
use dope_net::wire::{ReadyOpen, Reclaim, RecvChunk, RuntimeLimits, Wire};
use dope_test::{Gate, Wired};
use o3::buffer::{Borrowed, Bytes, RetainBytes};

const HANDSHAKE: &[u8] = b"hello";

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
        mut send: Storage<'a, SendBuf<1024>>,
        consumed: usize,
    ) -> Prepared<'a> {
        if self.established() && send.is_empty() && !self.pending.is_empty() {
            assert!(send.try_extend_from_slice(&self.pending));
            self.pending.clear();
        }
        if send.is_empty() {
            send.empty(consumed)
        } else {
            send.buffered(consumed)
        }
    }
}

impl Wire for DeferredWire {
    type Connection<'d> = Self;
    type ConnectionStorage = ();
    type InitConfig<'d> = ();
    type RuntimeContext<'d> = ();
    type Open<'a, 'd>
        = ReadyOpen<Self::Connection<'d>, Self::SendStorage>
    where
        'd: 'a;
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type RecvBatch<'a> = std::option::IntoIter<RecvChunk<'a, Self::Recv<'a>>>;
    type RetainedRecv<'d> = ProvidedView<'d>;
    type SendStorage = SendBuf<1024>;

    const RECLAIM: Reclaim = Reclaim::OnSubmit;

    const RAW_RECV: bool = true;

    fn connection_storage(_: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn runtime_context<'d>(_: RuntimeLimits, _: Self::InitConfig<'d>) -> std::io::Result<()>
    where
        Self: 'd,
    {
        Ok(())
    }

    fn prepare_open<'a, 'd>(_: &'a mut ()) -> Option<Self::Open<'a, 'd>>
    where
        'd: 'a,
    {
        Some(ReadyOpen::new(
            Self {
                rounds: 0,
                pending: Vec::new(),
            },
            SendBuf::new(),
        ))
    }

    fn holds_plain<'d>(wire: &Self::Connection<'d>, send: &Self::SendStorage) -> bool {
        !wire.pending.is_empty() || !send.is_empty()
    }

    fn process_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: &'a mut [u8],
    ) -> Self::RecvBatch<'a> {
        if !wire.established() {
            wire.rounds += 1;
            return None.into_iter();
        }
        Some(RecvChunk::Borrowed(Bytes::<Borrowed<'a>>::from(&*bytes))).into_iter()
    }

    fn process_retained_recv<'a, 'd>(
        wire: &mut Self::Connection<'d>,
        _: &mut (),
        bytes: ProvidedLease<'a>,
    ) -> Option<Self::RetainedRecv<'a>> {
        if !wire.established() {
            wire.rounds += 1;
            return None;
        }
        let span = bytes.span(0, bytes.as_slice().len())?;
        bytes.into_view(span).ok()
    }

    fn prepare_send<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        wire.pending.extend_from_slice(plain.as_slice());
        wire.prepare(send, plain.len())
    }

    fn prepare_send_vectored<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
        vectored: Vectored<'a>,
    ) -> Prepared<'a> {
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

    fn after_send<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        mut send: Storage<'a, Self::SendStorage>,
        sent: Sent,
    ) -> Prepared<'a> {
        assert!(send.try_consume(sent.get()));
        wire.prepare(send, 0)
    }

    fn flush_pending<'a, 'd>(
        wire: &'a mut Self::Connection<'d>,
        send: Storage<'a, Self::SendStorage>,
    ) -> Prepared<'a> {
        wire.prepare(send, 0)
    }
}

struct PreambleApp {
    frames: &'static [&'static [u8]],
    sends: Rc<RefCell<Vec<usize>>>,
    gate: Rc<Gate>,
}

impl PreambleApp {
    fn want_bytes(&self) -> usize {
        self.frames.iter().map(|f| f.len()).sum()
    }
}

impl<'d> Application<'d> for PreambleApp {
    type Conn = ();
    type Wire = DeferredWire;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, PreambleApp> for PreambleApp {
    fn accept(
        app: Pin<&mut PreambleApp>,
        slot: &mut Slot<'d, DeferredWire, listener::state::State<()>>,
        mut egress: listener::state::EgressCtx<'_, '_>,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        for frame in app.get_mut().frames {
            let mut buf = egress.write_buf_for(slot);
            buf[..frame.len()].copy_from_slice(frame);
            let ud = slot.token();
            slot.submit_buffered(buf, frame.len(), ud, driver);
        }
        Outcome::Ok
    }

    fn chunk<R: RetainBytes>(
        _app: Pin<&mut PreambleApp>,
        _slot: &mut Slot<'d, DeferredWire, listener::state::State<()>>,
        _egress: listener::state::EgressCtx<'_, '_>,
        _chunk: R,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }

    fn send(
        app: Pin<&mut PreambleApp>,
        slot: &mut Slot<'d, DeferredWire, listener::state::State<()>>,
        _egress: listener::state::EgressCtx<'_, '_>,
        sent: usize,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        let this = app.get_mut();
        this.sends.borrow_mut().push(sent);
        if this.sends.borrow().iter().sum::<usize>() >= this.want_bytes() {
            slot.set_close_after();
        }
    }

    fn close(
        app: Pin<&mut PreambleApp>,
        _slot: &mut Slot<'d, DeferredWire, listener::state::State<()>>,
        _egress: listener::state::EgressCtx<'_, '_>,
    ) {
        app.get_mut().gate.hit();
    }
}

#[test]
fn frames_written_before_wire_established_are_each_reported() {
    let gate = Gate::new();
    let sends = Rc::new(RefCell::new(Vec::new()));
    dope_test::tcp_case! {
        max_connections: 16,
        transport: dope_net::tcp::listener::Config::default(),
        env: Wired<DeferredWire>,
        app: PreambleApp {
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
                dope_test::read_all(s)
            });

            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            let want: Vec<u8> = FRAMES.concat();
            assert_eq!(
                got, want,
                "frames must reach the peer once and in order; a repeat means the slot \
         re-handed plaintext the wire had already consumed"
            );
            let lens: Vec<usize> = FRAMES.iter().map(|f| f.len()).collect();
            assert_eq!(
                *sends.borrow(),
                lens,
                "every frame must be reported to send exactly once"
            );
        }
    }
}
