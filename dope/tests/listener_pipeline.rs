#![cfg(target_os = "linux")]

mod common;

extern crate dope;
use std::pin::{Pin, pin};
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener, SlotEgress};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;
use o3::cell::BrandCell;

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

impl<'d> Application<'d> for PipelineApp {
    type Conn = ();
    type Wire = Identity;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: R,
        aux: &mut listener::Aux,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let ud = slot.token();
        for reply in [resp_a(), resp_b()] {
            let mut buf = aux.write_buf_for(slot);
            buf[..reply.len()].copy_from_slice(&reply);
            slot.submit_buffered(buf, reply.len(), ud, driver);
        }
        Outcome::CloseAfter
    }

    fn send(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
    }

    fn close(
        self: Pin<&mut Self>,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
        self.get_mut().gate.hit();
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, PipelineApp, Plain>,
}

#[test]
fn two_responses_committed_while_first_in_flight_arrive_in_order() {
    let mut want = resp_a();
    want.extend_from_slice(&resp_b());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, dope_net::tcp::listener::Config::default());
    exec.enter(|mut sess| {
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) = common::open_listener(
            PipelineApp { gate: gate.clone() },
            cfg,
            hash_builder,
            &mut sess.driver_access(),
        );
        let app = pin!(BrandCell::new(App { listener }));

        let peer = common::request_reply(addr, b"GO\n".to_vec());

        common::run_until(&mut sess, app.as_ref(), &gate, 1);
        let got = peer.join().expect("peer join");

        assert_eq!(
            got, want,
            "responses corrupted, reordered, or truncated on the pipelined path"
        );
        assert_eq!(gate.hits(), 1, "connection must close exactly once");
    });
}
