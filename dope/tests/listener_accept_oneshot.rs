#![cfg(target_os = "linux")]

mod common;

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::pin::pin;
use std::rc::Rc;

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener};
use dope::transport::config::tcp::ListenerOpts;
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};

use common::{Gate, Plain, TcpConfig};
use dope::Session;

#[derive(Default)]
struct Trace {
    accepted: RefCell<Vec<Option<IpAddr>>>,
    capped: RefCell<Vec<IpAddr>>,
}

struct TraceApp {
    trace: Rc<Trace>,
    gate: Rc<Gate>,
}

impl Application for TraceApp {
    type Conn = ();
    type Wire = Identity;

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
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) {
    }

    fn on_accept<'d>(
        &mut self,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
        _driver: &'d Driver,
    ) -> Outcome {
        self.trace.accepted.borrow_mut().push(slot.state.peer_ip());
        self.gate.hit();
        Outcome::Ok
    }

    fn on_capped(&mut self, peer_ip: IpAddr) {
        self.trace.capped.borrow_mut().push(peer_ip);
        self.gate.hit();
    }

    fn on_close<'d>(
        &mut self,
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, TraceApp, Plain>,
    #[pin]
    #[manifold]
    guard: common::Guard,
}

fn cap_opts(per_ip_cap: u32) -> ListenerOpts {
    ListenerOpts {
        per_ip_cap: Some(per_ip_cap),
        ..ListenerOpts::default()
    }
}

fn build<'d>(
    sess: &mut Session<'d>,
    cfg: TcpConfig,
    trace: Rc<Trace>,
    gate: Rc<Gate>,
) -> (App<'d>, SocketAddr) {
    let listener =
        Listener::<'d, 0, TraceApp, Plain>::open_in(TraceApp { trace, gate }, cfg, sess.driver())
            .expect("open_in");
    let addr = listener.local_addr().expect("local_addr");
    let app = App {
        listener,
        guard: common::Guard::new(),
    };
    (app, addr)
}

#[test]
fn accept_fires_once_per_connection_with_peer_ip() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, cap_opts(0));
    let mut sess = exec.enter();
    let (app, addr) = build(&mut sess, cfg, trace.clone(), gate.clone());
    let mut app = pin!(app);

    let _peer = common::connect(addr);

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 1);

    let accepted = trace.accepted.borrow();
    assert_eq!(accepted.len(), 1, "on_accept must fire exactly once");
    let want: IpAddr = "127.0.0.1".parse().expect("parse");
    assert_eq!(
        accepted[0],
        Some(want),
        "peer_ip must be recorded from the accept sockaddr"
    );
}

/// `per_ip_cap` counts the third loopback conn out; it is turned away before it
/// ever becomes a slot, so only `on_capped` sees it.
#[test]
fn per_ip_cap_turns_away_the_conn_past_the_cap() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, cap_opts(2));
    let mut sess = exec.enter();
    let (app, addr) = build(&mut sess, cfg, trace.clone(), gate.clone());
    let mut app = pin!(app);

    let _peers: Vec<TcpStream> = (0..3).map(|_| common::connect(addr)).collect();

    let guard = app.as_mut().guard_handle();
    common::run_until(&mut sess, app.as_mut(), guard, &gate, 3);

    let want: IpAddr = "127.0.0.1".parse().expect("parse");
    assert_eq!(trace.accepted.borrow().len(), 2, "per_ip_cap=2 admits two");
    assert_eq!(*trace.capped.borrow(), vec![want], "the third is capped");
}
