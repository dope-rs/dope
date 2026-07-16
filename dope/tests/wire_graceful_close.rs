#![cfg(target_os = "linux")]

mod common;

extern crate dope;
use o3::cell::BrandCell;

use std::io::Write;
use std::net::Shutdown;
use std::pin::{Pin, pin};
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener, SlotEgress};
use dope_net::link::slot::Slot;
use dope_net::wire::send::{Plain, Prepared, Storage, Vectored};
use dope_net::wire::{Reclaim, RuntimeLimits, Wire};
use o3::buffer::{Borrowed, Bytes, RetainBytes};

use common::{Gate, Wired};

const BYE: &[u8] = b"<<BYE>>";
const CONTROL: &[u8] = b"<<CONTROL>>";

struct GracefulWire;

impl Wire for GracefulWire {
    type InitConfig = ();
    type RuntimeContext = ();
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn runtime_context(_: RuntimeLimits) -> std::io::Result<()> {
        Ok(())
    }

    fn open(_: &(), _: &()) -> Option<(Self, ())> {
        Some((GracefulWire, ()))
    }

    fn process_recv<'a>(&mut self, _: &(), bytes: &'a [u8]) -> Option<Self::Recv<'a>> {
        Some(Bytes::<Borrowed<'a>>::from(bytes))
    }

    fn prepare_send<'a>(&'a mut self, _send: Storage<'a, ()>, plain: Plain<'a>) -> Prepared<'a> {
        let consumed = plain.len();
        Prepared::input(plain, consumed)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a>(&'a mut self, send: Storage<'a, ()>, _n: usize) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, ()>) -> Prepared<'a> {
        send.empty(0)
    }

    fn graceful_close<'a>(&'a mut self, _send: Storage<'a, ()>) -> Prepared<'a> {
        Prepared::static_slice(BYE)
    }
}

struct ProbeApp {
    payload: Option<Vec<u8>>,
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for ProbeApp {
    type Conn = ();
    type Wire = GracefulWire;

    fn chunk<R: RetainBytes>(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _chunk: R,
        aux: &mut listener::Aux,
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let Some(reply) = self.get_mut().payload.as_ref() else {
            return Outcome::Ok;
        };
        let n = reply.len();
        let mut buf = aux.write_buf_for(slot);
        buf[..n].copy_from_slice(reply);
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
    listener: Listener<'d, 0, ProbeApp, Wired<GracefulWire>>,
}

struct ControlWire {
    pending: bool,
}

impl Wire for ControlWire {
    type InitConfig = ();
    type RuntimeContext = ();
    type Recv<'a> = Bytes<Borrowed<'a>>;
    type SendStorage = ();

    const RECLAIM: Reclaim = Reclaim::OnComplete;

    fn runtime_context(_: RuntimeLimits) -> std::io::Result<()> {
        Ok(())
    }

    fn open(_: &(), _: &()) -> Option<(Self, ())> {
        Some((Self { pending: false }, ()))
    }

    fn process_recv<'a>(&mut self, _: &(), bytes: &'a [u8]) -> Option<Self::Recv<'a>> {
        self.pending = true;
        Some(Bytes::<Borrowed<'a>>::from(bytes))
    }

    fn prepare_send<'a>(&'a mut self, _send: Storage<'a, ()>, plain: Plain<'a>) -> Prepared<'a> {
        let consumed = plain.len();
        Prepared::input(plain, consumed)
    }

    fn prepare_send_vectored<'a>(
        &'a mut self,
        _send: Storage<'a, ()>,
        plain: Vectored<'a>,
    ) -> Prepared<'a> {
        let consumed = plain.bytes();
        Prepared::vectored(plain, consumed)
    }

    fn after_send<'a>(&'a mut self, send: Storage<'a, ()>, _n: usize) -> Prepared<'a> {
        send.empty(0)
    }

    fn flush_pending<'a>(&'a mut self, send: Storage<'a, ()>) -> Prepared<'a> {
        if std::mem::take(&mut self.pending) {
            Prepared::static_slice(CONTROL)
        } else {
            send.empty(0)
        }
    }
}

struct ControlApp {
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for ControlApp {
    type Conn = ();
    type Wire = ControlWire;

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
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
        slot.set_close_after();
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
struct ControlHost<'d> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, ControlApp, Wired<ControlWire>>,
}

#[test]
fn graceful_sentinel_trails_drain_reply() {
    let want = common::pattern(12_000);
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, dope_net::tcp::listener::Config::default());
    exec.enter(|mut sess| {
        let app = ProbeApp {
            payload: Some(want.clone()),
            gate: gate.clone(),
        };
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) =
            common::open_listener(app, cfg, hash_builder, &mut sess.driver_access());
        let app = pin!(BrandCell::new(App { listener }));

        let peer = common::spawn_peer(addr, |s| {
            s.write_all(b"GET\n").expect("request");
            common::read_all(s)
        });

        common::run_until(&mut sess, app.as_ref(), &gate, 1);
        let got = peer.join().expect("peer join");

        let mut expect = want;
        expect.extend_from_slice(BYE);
        assert_eq!(got, expect, "sentinel must trail the reply, before the FIN");
        assert_eq!(gate.hits(), 1, "connection must close exactly once");
    });
}

#[test]
fn graceful_sentinel_survives_peer_eof() {
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, dope_net::tcp::listener::Config::default());
    exec.enter(|mut sess| {
        let app = ProbeApp {
            payload: None,
            gate: gate.clone(),
        };
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) =
            common::open_listener(app, cfg, hash_builder, &mut sess.driver_access());
        let app = pin!(BrandCell::new(App { listener }));

        let peer = common::spawn_peer(addr, |s| {
            s.write_all(b"REQ").expect("request");
            s.shutdown(Shutdown::Write).expect("half close");
            common::read_all(s)
        });

        common::run_until(&mut sess, app.as_ref(), &gate, 1);
        let got = peer.join().expect("peer join");

        assert_eq!(got, BYE, "peer EOF must not suppress the graceful sentinel");
        assert_eq!(gate.hits(), 1, "connection must close exactly once");
    });
}

#[test]
fn control_output_is_flushed_after_plaintext() {
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, dope_net::tcp::listener::Config::default());
    exec.enter(|mut sess| {
        let app = ControlApp { gate: gate.clone() };
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) =
            common::open_listener(app, cfg, hash_builder, &mut sess.driver_access());
        let app = pin!(BrandCell::new(ControlHost { listener }));

        let peer = common::spawn_peer(addr, |s| {
            s.write_all(b"REQ").expect("request");
            common::read_all(s)
        });

        common::run_until(&mut sess, app.as_ref(), &gate, 1);
        let got = peer.join().expect("peer join");

        assert_eq!(got, CONTROL);
        assert_eq!(gate.hits(), 1);
    });
}
