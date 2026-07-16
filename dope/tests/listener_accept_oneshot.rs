#![cfg(target_os = "linux")]

mod common;

extern crate dope;
use o3::cell::BrandCell;

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::pin::{Pin, pin};
use std::rc::Rc;

use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application, Listener};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use o3::buffer::RetainBytes;

use common::{Gate, Plain};

#[derive(Default)]
struct Trace {
    accepted: RefCell<Vec<Option<IpAddr>>>,
    capped: RefCell<Vec<IpAddr>>,
}

struct TraceApp {
    trace: Rc<Trace>,
    gate: Rc<Gate>,
}

impl<'d> Application<'d> for TraceApp {
    type Conn = ();
    type Wire = Identity;

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
        _slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) {
    }

    fn accept(
        self: Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, listener::State<Self::Conn>>,
        _aux: &mut listener::Aux,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.trace.accepted.borrow_mut().push(slot.state.peer_ip());
        this.gate.hit();
        Outcome::Ok
    }

    fn capped(self: Pin<&mut Self>, peer_ip: IpAddr) {
        let this = self.get_mut();
        this.trace.capped.borrow_mut().push(peer_ip);
        this.gate.hit();
    }

    fn close(
        self: Pin<&mut Self>,
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
}

fn listener_config(per_ip_limit: u32) -> dope_net::tcp::listener::Config {
    dope_net::tcp::listener::Config {
        per_ip_limit: Some(per_ip_limit),
        ..dope_net::tcp::listener::Config::default()
    }
}

fn connect_n(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
    (0..n).map(|_| common::connect(addr)).collect()
}

#[test]
fn accept_fires_once_per_connection_with_peer_ip() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, listener_config(0));
    exec.enter(|mut sess| {
        let trace_app = TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        };
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) =
            common::open_listener(trace_app, cfg, hash_builder, &mut sess.driver_access());
        let app = pin!(BrandCell::new(App { listener }));

        let _peer = common::connect(addr);

        common::run_until(&mut sess, app.as_ref(), &gate, 1);

        let accepted = trace.accepted.borrow();
        assert_eq!(accepted.len(), 1, "accept must fire exactly once");
        let want: IpAddr = "127.0.0.1".parse().expect("parse");
        assert_eq!(
            accepted[0],
            Some(want),
            "peer_ip must be recorded from the accept sockaddr"
        );
    });
}

#[test]
fn per_ip_limit_turns_away_excess_connections() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    let (exec, cfg) = common::tcp_host(64, listener_config(2));
    exec.enter(|mut sess| {
        let trace_app = TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        };
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let (listener, addr) =
            common::open_listener(trace_app, cfg, hash_builder, &mut sess.driver_access());
        let app = pin!(BrandCell::new(App { listener }));

        let _peers = connect_n(addr, 3);

        common::run_until(&mut sess, app.as_ref(), &gate, 3);

        let want: IpAddr = "127.0.0.1".parse().expect("parse");
        assert_eq!(
            trace.accepted.borrow().len(),
            2,
            "per_ip_limit=2 admits two"
        );
        assert_eq!(*trace.capped.borrow(), vec![want], "the third is capped");
    });
}
