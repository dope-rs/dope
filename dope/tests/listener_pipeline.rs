#![cfg(target_os = "linux")]
//! Two responses committed in one `on_chunk`: the first goes in-flight on the
//! fast path, the second is requested while it still is. `write_buf_for` must
//! hand back scratch (not the in-flight buffer) and `submit_buffered` must queue
//! it, so both arrive intact and in order.

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

fn resp_a() -> Vec<u8> {
    vec![0xA1; 8000]
}

fn resp_b() -> Vec<u8> {
    vec![0xB2; 9000]
}

struct PipelineApp {
    gate: Rc<Gate>,
}

impl Application for PipelineApp {
    type Conn = ();
    type Wire = Identity;

    fn on_chunk<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &'d Driver,
    ) -> Outcome {
        let ud = slot.token();
        for reply in [resp_a(), resp_b()] {
            let buf = aux.write_buf_for(slot);
            buf[..reply.len()].copy_from_slice(&reply);
            slot.submit_buffered(buf, reply.len(), ud, driver);
        }
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
    listener: Listener<'d, 0, PipelineApp, Plain>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

#[test]
fn two_responses_committed_while_first_in_flight_arrive_in_order() {
    let mut want = resp_a();
    want.extend_from_slice(&resp_b());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, ListenerOpts::default());
    let mut sess = exec.enter();
    let listener = Listener::<0, PipelineApp, Plain>::open_in(
        PipelineApp { gate: gate.clone() },
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
        s.write_all(b"GO\n").expect("request");
        let mut got = Vec::new();
        s.read_to_end(&mut got).expect("read to eof");
        got
    });

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);
    let got = peer.join().expect("peer join");

    assert_eq!(
        got, want,
        "responses corrupted, reordered, or truncated on the pipelined path"
    );
    assert_eq!(gate.hits(), 1, "connection must close exactly once");
}
