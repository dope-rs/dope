#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::pin::Pin;
use std::rc::Rc;

use dope::manifold::listener::application::{Application, ApplicationHooks};
use dope::manifold::listener::state::EgressCtx;
use dope::manifold::{Outcome, listener};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use dope_test::Gate;
use o3::buffer::RetainBytes;

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
    type Hooks = Self;
}

impl<'d> ApplicationHooks<'d, TraceApp> for TraceApp {
    fn chunk<R: RetainBytes>(
        _app: Pin<&mut TraceApp>,
        _slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _chunk: R,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        Outcome::Ok
    }

    fn accept(
        app: Pin<&mut TraceApp>,
        slot: &mut Slot<'d, Identity, listener::state::State<()>>,
        _egress: EgressCtx<'_, 'd, '_>,
        _driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Outcome {
        let this = app.get_mut();
        this.trace.accepted.borrow_mut().push(slot.state.peer_ip());
        this.gate.hit();
        Outcome::Ok
    }

    fn capped(app: Pin<&mut TraceApp>, peer_ip: IpAddr) {
        let this = app.get_mut();
        this.trace.capped.borrow_mut().push(peer_ip);
        this.gate.hit();
    }
}

fn listener_config(per_ip_limit: u32) -> dope_net::tcp::listener::Config {
    dope_net::tcp::listener::Config {
        per_ip_limit: Some(per_ip_limit),
        ..dope_net::tcp::listener::Config::default()
    }
}

fn connect_n(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
    (0..n).map(|_| dope_test::connect(addr)).collect()
}

#[test]
fn accept_fires_once_per_connection_with_peer_ip() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        transport: listener_config(0),
        app: TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        },
        |case| {
            let _peer = dope_test::connect(case.addr());
            case.until(&gate, 1);

            let accepted = trace.accepted.borrow();
            assert_eq!(accepted.len(), 1, "accept must fire exactly once");
            let want: IpAddr = "127.0.0.1".parse().expect("parse");
            assert_eq!(
                accepted[0],
                Some(want),
                "peer_ip must be recorded from the accept sockaddr"
            );
        }
    }
}

#[test]
fn per_ip_limit_turns_away_excess_connections() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    dope_test::tcp_case! {
        max_connections: 64,
        transport: listener_config(2),
        app: TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        },
        |case| {
            let _peers = connect_n(case.addr(), 3);
            case.until(&gate, 3);

            let want: IpAddr = "127.0.0.1".parse().expect("parse");
            assert_eq!(
                trace.accepted.borrow().len(),
                2,
                "per_ip_limit=2 admits two"
            );
            assert_eq!(*trace.capped.borrow(), vec![want], "the third is capped");
        }
    }
}
