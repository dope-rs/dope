use std::{
    cell::Cell,
    net::{SocketAddr, TcpStream},
    pin::Pin,
    rc::Rc,
    task,
};

use dope_fiber::{abi, context};
use dope_manifold::{
    Bundle, Outcome,
    listener::{config::Admission, connection, handler::Application},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, peer::Peer, scenario::scenarios::Listener};
use o3::buffer::bytes::Retainable;

#[derive(Default)]
struct Trace {
    accepted: Cell<usize>,
}

struct TraceApp {
    trace: Rc<Trace>,
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for TraceApp {
    type Conn = ();
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.trace.accepted.set(this.trace.accepted.get() + 1);
        this.gate.hit();
        Outcome::Ok
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID> for TraceApp {
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

fn listener_config(per_ip_limit: u32) -> dope_net::tcp::ListenerConfig {
    dope_net::tcp::ListenerConfig {
        per_ip_limit: Some(per_ip_limit),
        ..dope_net::tcp::ListenerConfig::default()
    }
}

fn connect_n(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
    (0..n).map(|_| Peer::at(addr).connect()).collect()
}

struct Turns(u32);

struct SingleAdmission;

impl Admission for SingleAdmission {
    const PER_IP_LIMIT: u32 = 1;
}

impl<'d> abi::Fiber<'d> for Turns {
    type Output = ();

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<()> {
        let (mut self_, cx) = call.into_parts();
        if self_.0 == 0 {
            return task::Poll::Ready(());
        }
        self_.0 -= 1;
        cx.wake();
        task::Poll::Pending
    }
}

struct ActivationApp {
    activated: Rc<Cell<usize>>,
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for ActivationApp {
    type Conn = ();
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        connection.wake_target().wake();
        Outcome::Ok
    }

    fn activate(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) {
        let this = self.get_mut();
        this.activated.set(this.activated.get() + 1);
        this.gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for ActivationApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

#[test]
fn explicit_connection_readiness_reaches_the_typed_application_callback() {
    let activated = Rc::new(Cell::new(0));
    let gate = Gate::new();
    Listener::new(8, listener_config(0)).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        ActivationApp {
            activated: Rc::clone(&activated),
            gate: gate.clone(),
        },
        |case| {
            let _peer = Peer::at(case.addr()).connect();
            case.until(&gate, 1);
            assert_eq!(activated.get(), 1);
        },
    );
}

#[test]
fn accept_fires_once_per_connection() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    Listener::new(64, listener_config(0)).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        },
        |case| {
            let _peer = Peer::at(case.addr()).connect();
            case.until(&gate, 1);

            assert_eq!(trace.accepted.get(), 1, "accept must fire exactly once");
        },
    );
}

#[test]
fn accept_fires_after_nonempty_stream_tuning() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    let stream = dope_net::tcp::StreamConfig {
        no_delay: Some(true),
        ..Default::default()
    };
    Listener::new(64, listener_config(0))
        .stream(stream)
        .run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
            TraceApp {
                trace: trace.clone(),
                gate: gate.clone(),
            },
            |case| {
                let _peer = Peer::at(case.addr()).connect();
                case.until(&gate, 1);

                assert_eq!(trace.accepted.get(), 1, "tuned accept must publish once");
            },
        );
}

#[test]
fn per_ip_limit_turns_away_excess_connections() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    Listener::new(64, listener_config(2)).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        },
        |case| {
            let mut peers = connect_n(case.addr(), 3);
            case.until(&gate, 2);
            case.drive(Turns(64));
            assert_eq!(trace.accepted.get(), 2, "per_ip_limit=2 admits two");
            peers.clear();
        },
    );
}

#[test]
fn admission_default_is_independent_of_timing() {
    let trace = Rc::new(Trace::default());
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<
        Tcp,
        Identity,
        Throughput,
        Throughput,
        SingleAdmission,
    >, _>(
        TraceApp {
            trace: trace.clone(),
            gate: gate.clone(),
        },
        |case| {
            let mut peers = connect_n(case.addr(), 2);
            case.until(&gate, 1);
            case.drive(Turns(64));
            assert_eq!(trace.accepted.get(), 1);
            peers.clear();
        },
    );
}
