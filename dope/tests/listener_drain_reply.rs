#![cfg(target_os = "linux")]

extern crate dope;

use std::pin::Pin;
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener;
use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::egress::SlotEgress;
use dope::manifold::listener::state::EgressCtx;
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use dope_test::Gate;
use o3::buffer::RetainBytes;

struct ReplyApp {
    payload: Vec<u8>,
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for ReplyApp {
    type Conn = ();
    type Wire = Identity;
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, ReplyApp> for ReplyApp {
    fn chunk<R: RetainBytes>(
        app: Pin<&mut ReplyApp>,
        slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        mut egress: EgressCtx<'_, '_>,
        _chunk: R,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let payload = &app.get_mut().payload;
        let n = payload.len();
        let mut buf = egress.write_buf_for(slot);
        buf[..n].copy_from_slice(payload);
        let ud = slot.token();
        slot.submit_buffered(buf, n, ud, driver);
        Outcome::CloseAfter
    }

    fn close(
        app: Pin<&mut ReplyApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, '_>,
    ) {
        app.get_mut().gate.hit();
    }
}

#[test]
fn drain_reply_is_delivered_in_full_before_close() {
    let want = dope_test::pattern(12_000);
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        app: ReplyApp {
            payload: want.clone(),
            gate: gate.clone(),
        },
        |case| {
            let peer = case.request_reply(b"GET\n".to_vec());
            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(got, want, "reply truncated or corrupted on the drain path");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        }
    }
}
