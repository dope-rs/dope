#![cfg(target_os = "linux")]
//! `Wire::on_graceful_close` gets to trail bytes after the app's reply and before
//! the FIN — and still fires when the peer half-closes first.

mod common;

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::pin::pin;
use std::rc::Rc;

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener};
use dope::token::Token;
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::{Core, Slot};
use dope::transport::wire::{Reclaim, RecvChunk, Vectored, Wire};

use common::{Gate, TcpConfig, Wired};
use dope::Session;

const BYE: &[u8] = b"<<BYE>>";

struct GracefulWire;

impl Wire for GracefulWire {
    type InitConfig = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn new(_: &()) -> Self {
        GracefulWire
    }

    fn process_recv<'a>(&mut self, bytes: &'a [u8]) -> Option<RecvChunk<'a>> {
        Some(RecvChunk::Borrowed(bytes))
    }

    fn submit_send<'d>(
        &mut self,
        core: &mut Core<'d>,
        plain: &[u8],
        ud: Token,
        driver: &'d Driver,
    ) -> usize {
        if plain.is_empty() {
            return 0;
        }
        core.submit_single(ud, plain, driver);
        plain.len()
    }

    fn submit_send_vectored<'d>(
        &mut self,
        core: &mut Core<'d>,
        vectored: Vectored<'_>,
        ud: Token,
        driver: &'d Driver,
    ) -> usize {
        if vectored.iovs.is_empty() {
            return 0;
        }
        let consumed: usize = vectored.iovs.iter().map(|v| v.len()).sum();
        core.submit_vectored(ud, vectored, driver);
        consumed
    }

    fn after_send_cqe<'d>(
        &mut self,
        _core: &mut Core<'d>,
        _n: usize,
        _ud: Token,
        _driver: &'d Driver,
    ) -> bool {
        false
    }

    fn flush_pending<'d>(&mut self, _core: &mut Core<'d>, _ud: Token, _driver: &'d Driver) {}

    fn on_graceful_close<'d>(&mut self, core: &mut Core<'d>, ud: Token, driver: &'d Driver) {
        core.submit_single(ud, BYE, driver);
    }
}

fn payload() -> Vec<u8> {
    (0..12_000u32).map(|i| (i % 251) as u8).collect()
}

struct ProbeApp {
    payload: Option<Vec<u8>>,
    gate: Rc<Gate>,
}

impl Application for ProbeApp {
    type Conn = ();
    type Wire = GracefulWire;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &'d Driver,
    ) -> Outcome {
        let Some(reply) = self.payload.as_ref() else {
            return Outcome::Ok;
        };
        let n = reply.len();
        let buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(reply);
        let ud = slot.token();
        slot.submit_buffered(buf, n, ud, driver);
        Outcome::CloseAfter
    }

    fn on_send<'d>(
        &mut self,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) {
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
    listener: Listener<'d, 0, ProbeApp, Wired<GracefulWire>>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

fn build<'d>(
    sess: &mut Session<'d>,
    cfg: TcpConfig,
    payload: Option<Vec<u8>>,
    gate: Rc<Gate>,
) -> (App<'d>, SocketAddr) {
    let listener = Listener::<0, ProbeApp, Wired<GracefulWire>>::open_in(
        ProbeApp { payload, gate },
        cfg,
        sess.driver(),
    )
    .expect("open_in");
    let addr = listener.local_addr().expect("local_addr");
    let app = App {
        listener,
        guard: common::Guard::new(),
    };
    (app, addr)
}

#[test]
fn graceful_sentinel_trails_drain_reply() {
    let want = payload();
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, ListenerOpts::default());
    let mut sess = exec.enter();
    let (app, addr) = build(&mut sess, cfg, Some(want.clone()), gate.clone());
    let mut app = pin!(app);

    let peer = std::thread::spawn(move || {
        let mut s = common::connect(addr);
        s.write_all(b"GET\n").expect("request");
        let mut got = Vec::new();
        s.read_to_end(&mut got).expect("read to eof");
        got
    });

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let got = peer.join().expect("peer join");

    let mut expect = want;
    expect.extend_from_slice(BYE);
    assert_eq!(got, expect, "sentinel must trail the reply, before the FIN");
    assert_eq!(gate.hits(), 1, "connection must close exactly once");
}

#[test]
fn graceful_sentinel_survives_peer_eof() {
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, ListenerOpts::default());
    let mut sess = exec.enter();
    let (app, addr) = build(&mut sess, cfg, None, gate.clone());
    let mut app = pin!(app);

    let peer = std::thread::spawn(move || {
        let mut s = common::connect(addr);
        s.write_all(b"REQ").expect("request");
        s.shutdown(Shutdown::Write).expect("half close");
        let mut got = Vec::new();
        s.read_to_end(&mut got).expect("read to eof");
        got
    });

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let got = peer.join().expect("peer join");

    assert_eq!(got, BYE, "peer EOF must not suppress the graceful sentinel");
    assert_eq!(gate.hits(), 1, "connection must close exactly once");
}
