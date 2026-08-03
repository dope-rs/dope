#![cfg(target_os = "linux")]

extern crate dope;
use std::pin::Pin;
use std::rc::Rc;

use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::egress::SlotEgress;
use dope::manifold::listener::state::EgressCtx;
use dope::manifold::{Outcome, listener};
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
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, PipelineApp> for PipelineApp {
    fn chunk<R: RetainBytes>(
        _app: Pin<&mut PipelineApp>,
        slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        mut egress: EgressCtx<'_, 'd, '_>,
        _chunk: R,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let ud = slot.token();
        for reply in [resp_a(), resp_b()] {
            let mut buf = egress.write_buf_for(slot);
            buf[..reply.len()].copy_from_slice(&reply);
            slot.submit_buffered(buf, reply.len(), ud, driver);
        }
        Outcome::CloseAfter
    }

    fn close(
        app: Pin<&mut PipelineApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
    ) {
        app.get_mut().gate.hit();
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
