#![cfg(target_os = "linux")]

extern crate dope;
use std::pin::Pin;
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, SlotEgress};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use dope_test::Gate;
use o3::buffer::RetainBytes;

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

#[test]
fn two_responses_committed_while_first_in_flight_arrive_in_order() {
    let mut want = resp_a();
    want.extend_from_slice(&resp_b());
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        app: PipelineApp { gate: gate.clone() },
        |case| {
            let peer = case.request_reply(b"GO\n".to_vec());
            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(
                got, want,
                "responses corrupted, reordered, or truncated on the pipelined path"
            );
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        }
    }
}
