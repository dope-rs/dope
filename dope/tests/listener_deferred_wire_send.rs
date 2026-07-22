#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::RefCell;
use std::io::Write;
use std::pin::Pin;
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, SlotEgress};
use dope_net::link::slot::Slot;
use dope_net::wire::send::{Plain, Prepared, SendBuf, Storage, Vectored};
use dope_net::wire::{Reclaim, RuntimeLimits, Wire};
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
            send.extend_from_slice(&self.pending);
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
    type InitConfig = ();
    type RuntimeContext = ();
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = SendBuf<1024>;

    const RECLAIM: Reclaim = Reclaim::OnSubmit;

    const RAW_RECV: bool = true;

    fn runtime_context(_: RuntimeLimits) -> std::io::Result<()> {
        Ok(())
    }

    fn open(_: &(), _: &()) -> Option<(Self, Self::SendStorage)> {
        Some((
            Self {
                rounds: 0,
                pending: Vec::new(),
            },
            SendBuf::new(),
        ))
    }

    fn holds_plain(&self, send: &Self::SendStorage) -> bool {
        !self.pending.is_empty() || !send.is_empty()
    }

    fn process_recv<'a>(&mut self, _: &(), bytes: &'a [u8]) -> Option<Self::Recv<'a>> {
        if !self.established() {
            self.rounds += 1;
            return None;
        }
        Some(Bytes::<Borrowed<'a>>::from(bytes))
    }

    fn prepare_send<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        plain: Plain<'a>,
    ) -> Prepared<'a> {
        self.pending.extend_from_slice(plain.as_slice());
        self.prepare(send, plain.len())
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        send: Storage<'a, Self::SendStorage>,
        vectored: Vectored<'a>,
    ) -> Prepared<'a> {
        let mut consumed = 0;
        for plain in vectored.iter() {
            if plain.is_empty() {
                continue;
            }
            self.pending.extend_from_slice(plain);
            consumed += plain.len();
        }
        self.prepare(send, consumed)
    }

    fn after_send<'a>(
        &'a mut self,
        mut send: Storage<'a, Self::SendStorage>,
        n: usize,
    ) -> Prepared<'a> {
        send.consume(n);
        self.prepare(send, 0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, Self::SendStorage>) -> Prepared<'a> {
        self.prepare(send, 0)
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

    fn accept(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        aux: &mut listener::Aux,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        for frame in self.get_mut().frames {
            let mut buf = aux.write_buf_for(slot);
            buf[..frame.len()].copy_from_slice(frame);
            let ud = slot.token();
            slot.submit_buffered(buf, frame.len(), ud, driver);
        }
        Outcome::Ok
    }

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: R,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }

    fn send(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        let this = self.get_mut();
        this.sends.borrow_mut().push(sent);
        if this.sends.borrow().iter().sum::<usize>() >= this.want_bytes() {
            slot.set_close_after();
        }
    }

    fn close(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
        self.get_mut().gate.hit();
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
