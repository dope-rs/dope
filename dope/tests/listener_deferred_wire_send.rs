#![cfg(target_os = "linux")]
//! A wire that consumes plaintext without arming a socket send — what a TLS wire
//! does mid-handshake: app data is stashed, no ciphertext exists yet. h2-over-TLS
//! trips it, because the h2 server writes SETTINGS the moment the connection
//! opens. Plaintext h2c and HTTP/1 only write post-handshake and never do.

mod common;

use std::cell::RefCell;
use std::io::{Read, Write};
use std::pin::pin;
use std::rc::Rc;

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::token::Token;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::{Core, Slot};
use dope::transport::wire::{Reclaim, RecvChunk, Vectored, Wire};

use common::{Gate, Wired};

const HANDSHAKE: &[u8] = b"hello";

/// Rounds the peer must send before the wire establishes. Real TLS takes more
/// than one, which is what keeps the wire holding plaintext across a recv.
const ROUNDS: u8 = 2;

struct DeferredWire {
    rounds: u8,
    pending: Vec<u8>,
    inflight: Vec<u8>,
}

impl DeferredWire {
    fn established(&self) -> bool {
        self.rounds >= ROUNDS
    }

    fn arm<'d>(&mut self, core: &mut Core<'d>, ud: Token, driver: &'d Driver) {
        if !self.established() || core.is_send_inflight() || self.pending.is_empty() {
            return;
        }
        self.inflight.clear();
        self.inflight.append(&mut self.pending);
        core.submit_single(ud, &self.inflight, driver);
    }
}

impl Wire for DeferredWire {
    type InitConfig = ();

    const RECLAIM: Reclaim = Reclaim::OnSubmit;

    const RAW_RECV: bool = true;

    fn new(_: &()) -> Self {
        Self {
            rounds: 0,
            pending: Vec::new(),
            inflight: Vec::new(),
        }
    }

    fn holds_plain(&self) -> bool {
        !self.pending.is_empty()
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>> {
        if !self.established() {
            self.rounds += 1;
            return None;
        }
        Some(RecvChunk::Borrowed(bytes))
    }

    fn submit_send<'d>(
        &mut self,
        core: &mut Core<'d>,
        plain: &[u8],
        ud: Token,
        driver: &'d Driver,
    ) -> usize {
        self.pending.extend_from_slice(plain);
        self.arm(core, ud, driver);
        plain.len()
    }

    fn submit_send_vectored<'d>(
        &mut self,
        core: &mut Core<'d>,
        vectored: Vectored<'_>,
        ud: Token,
        driver: &'d Driver,
    ) -> usize {
        let mut consumed = 0;
        for iov in vectored.iovs {
            if iov.is_empty() {
                continue;
            }
            // SAFETY: iov points into caller-owned memory valid for this call.
            let plain = unsafe { std::slice::from_raw_parts(iov.as_ptr(), iov.len()) };
            self.pending.extend_from_slice(plain);
            consumed += plain.len();
        }
        self.arm(core, ud, driver);
        consumed
    }

    fn after_send_cqe<'d>(
        &mut self,
        _core: &mut Core<'d>,
        _n: usize,
        _ud: Token,
        _driver: &'d Driver,
    ) -> bool {
        self.inflight.clear();
        false
    }

    fn flush_pending<'d>(&mut self, core: &mut Core<'d>, ud: Token, driver: &'d Driver) {
        self.arm(core, ud, driver);
    }
}

/// Writes every frame at connection open — before the wire is established.
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

impl Application for PreambleApp {
    type Conn = ();
    type Wire = DeferredWire;

    fn on_accept<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        aux: &mut listener::Aux,
        driver: &'d Driver,
    ) -> Outcome {
        for frame in self.frames {
            let buf = aux.write_buf_for(slot);
            buf[..frame.len()].copy_from_slice(frame);
            let ud = slot.token();
            slot.submit_buffered(buf, frame.len(), ud, driver);
        }
        Outcome::Ok
    }

    fn on_chunk<'d>(
        &mut self,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) -> Outcome {
        Outcome::Ok
    }

    fn on_send<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        sent: usize,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) {
        self.sends.borrow_mut().push(sent);
        if self.sends.borrow().iter().sum::<usize>() >= self.want_bytes() {
            slot.core.set_close_after();
        }
    }

    fn on_close<'d>(
        &mut self,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
        self.gate.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, PreambleApp, Wired<DeferredWire>>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

/// Every frame reaches the peer once, in order, and each is reported to
/// `on_send` exactly once.
fn run_case(frames: &'static [&'static [u8]]) {
    let gate = Gate::new();
    let sends = Rc::new(RefCell::new(Vec::new()));
    let (exec, cfg) = common::tcp_host(16, ListenerOpts::default());
    let mut sess = exec.enter();
    let listener = Listener::<0, PreambleApp, Wired<DeferredWire>>::open_in(
        PreambleApp {
            frames,
            sends: sends.clone(),
            gate: gate.clone(),
        },
        cfg,
        sess.driver(),
    )
    .expect("open_in");
    let addr = listener.local_addr().expect("local_addr");
    let mut app = pin!(App {
        listener,
        guard: common::Guard::new(),
    });

    let peer = std::thread::spawn(move || {
        let mut s = common::connect(addr);
        for _ in 0..ROUNDS {
            s.write_all(HANDSHAKE).expect("handshake");
            // a distinct recv per round; coalesced writes would establish in one
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let mut got = Vec::new();
        s.read_to_end(&mut got).expect("read to eof");
        got
    });

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let got = peer.join().expect("peer join");

    let want: Vec<u8> = frames.concat();
    assert_eq!(
        got, want,
        "frames must reach the peer once and in order; a repeat means the slot \
         re-handed plaintext the wire had already consumed"
    );
    let lens: Vec<usize> = frames.iter().map(|f| f.len()).collect();
    assert_eq!(
        *sends.borrow(),
        lens,
        "every frame must be reported to on_send exactly once"
    );
}

#[test]
fn frame_written_before_wire_established_is_sent_once() {
    run_case(&[b"PREAMBLE"]);
}

/// The second frame stages on the deferred queue while the wire still holds the
/// first. Handing it over before the wire flushes coalesces both into one CQE,
/// and the second frame's `on_send` never fires.
#[test]
fn frames_written_before_wire_established_are_each_reported() {
    run_case(&[b"PREAMBLE", b"SETTINGS"])
}
