#![cfg(target_os = "linux")]

mod common;

extern crate dope;
use o3::cell::BrandCell;

use std::pin::{Pin, pin};
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener, SlotEgress};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;

use common::{Gate, Plain};

struct ReplyApp {
    payload: Vec<u8>,
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for ReplyApp {
    type Conn = ();
    type Wire = Identity;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: R,
        aux: &mut listener::Aux,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let payload = &self.get_mut().payload;
        let n = payload.len();
        let mut buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(payload);
        let ud = slot.token();
        slot.submit_buffered(buf, n, ud, driver);
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
    listener: Listener<'d, 0, ReplyApp, Plain>,
}

#[test]
fn drain_reply_is_delivered_in_full_before_close() {
    let want = common::pattern(12_000);
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, dope_net::tcp::listener::Config::default());
    exec.enter(|mut sess| {
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) = common::open_listener(
            ReplyApp {
                payload: want.clone(),
                gate: gate.clone(),
            },
            cfg,
            hash_builder,
            &mut sess.driver_access(),
        );
        let app = pin!(BrandCell::new(App { listener }));

        let peer = common::request_reply(addr, b"GET\n".to_vec());

        common::run_until(&mut sess, app.as_ref(), &gate, 1);
        let got = peer.join().expect("peer join");

        assert_eq!(got, want, "reply truncated or corrupted on the drain path");
        assert_eq!(gate.hits(), 1, "connection must close exactly once");
    });
}
