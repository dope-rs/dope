#![cfg(target_os = "linux")]
//! A buffered reply staged on the close-after (Draining) path must reach the peer
//! in full before the FIN; a truncated close shows up as a short client read.

mod common;

use std::io::{Read, Write};
use std::pin::pin;
use std::rc::Rc;

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener};
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};

use common::{Gate, Plain};

fn payload() -> Vec<u8> {
    (0..12_000u32).map(|i| (i % 251) as u8).collect()
}

struct ReplyApp {
    payload: Vec<u8>,
    gate: Rc<Gate>,
}

impl Application for ReplyApp {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &'d Driver,
    ) -> Outcome {
        let n = self.payload.len();
        let buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(&self.payload);
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
    listener: Listener<'d, 0, ReplyApp, Plain>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

#[test]
fn drain_reply_is_delivered_in_full_before_close() {
    let want = payload();
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, ListenerOpts::default());
    let mut sess = exec.enter();
    let listener = Listener::<0, ReplyApp, Plain>::open_in(
        ReplyApp {
            payload: want.clone(),
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
        s.write_all(b"GET\n").expect("request");
        let mut got = Vec::new();
        s.read_to_end(&mut got).expect("read to eof");
        got
    });

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let got = peer.join().expect("peer join");

    assert_eq!(got, want, "reply truncated or corrupted on the drain path");
    assert_eq!(gate.hits(), 1, "connection must close exactly once");
}
