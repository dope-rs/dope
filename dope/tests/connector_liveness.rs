//! Anchor test for the connector's inbound-idle liveness watchdog.
//!
//! Reproduces the outage class at the runtime layer: a peer that goes silent
//! WITHOUT closing (half-open / a broker maintenance window that just stops
//! answering) never surfaces a readable EOF, so nothing event-driven detects it.
//! The connector's inbound-idle deadline is the sole detector — on expiry it
//! must force a *recoverable* reconnect (redial), not wait on the dead socket
//! forever. Real reactor clock, real loopback sockets: no mocks, no unit test.
//!
//! Lives in `dope/tests` (the layer that OWNS the behavior) so a future
//! connector/backend refactor keeps it green — the guarantee can't silently
//! regress the way it did when it was reimplemented per-protocol.
#![cfg(target_os = "linux")]

mod common;

extern crate dope;

use std::cell::Cell;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::rc::Rc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dope::DriverContext;
use dope::manifold::connector::source::{DialKey, Static};
use dope::manifold::connector::state::State;
use dope::manifold::connector::{ChunkOutcome, ConnApp, Core};
use dope::runtime::Executor;
use dope::runtime::profile::Throughput;
use dope::{driver, hash};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::buffer::{RetainBytes, Shared};
use o3::cell::BrandCell;

use common::Gate;

const ID: u8 = 0;
// Exactly one upstream connection, so `connected()` fires once per real dial —
// the reconnect is unambiguous. (With N slots the connector eagerly opens N
// connections to the single addr at startup, which would satisfy the gate
// without ever exercising liveness.)
const MAX: usize = 1;
const IDLE_TIMEOUT: Duration = Duration::from_millis(150);
const REDIAL_BACKOFF: Duration = Duration::from_millis(20);

type Slot<'d> = dope_net::link::slot::Slot<'d, Identity, State<(), Shared>>;

/// Minimal `ConnApp` that opts into liveness and records established connections.
struct LivenessApp {
    timeout: Option<Duration>,
    gate: Rc<Gate>,
    start: Instant,
    /// Wall-clock offset of the 2nd `connected()` — i.e. when the reconnect
    /// completed. Used to prove the reconnect was driven by the idle deadline
    /// (≈ `IDLE_TIMEOUT`) and not some spurious fast redial.
    reconnect_at: Rc<Cell<Option<Duration>>>,
}

impl<'d> ConnApp<'d> for LivenessApp {
    type Conn = ();
    type Wire = Identity;
    type Send = Shared;

    fn inbound_idle_timeout(&self) -> Option<Duration> {
        self.timeout
    }

    fn chunk<R: RetainBytes>(
        &mut self,
        _slot: &mut Slot<'d>,
        _chunk: R,
        _driver: &mut DriverContext<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::Ok
    }

    fn connected(
        &mut self,
        _key: DialKey,
        _slot: &mut Slot<'d>,
        _driver: &mut DriverContext<'_, 'd>,
    ) {
        self.gate.hit();
        if self.gate.hits() == 2 {
            self.reconnect_at.set(Some(self.start.elapsed()));
        }
    }

    fn send(&mut self, _slot: &mut Slot<'d>, _sent: usize, _driver: &mut DriverContext<'_, 'd>) {}

    fn close(&mut self, _slot: &mut Slot<'d>) {}
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App<'d> {
    #[pin]
    #[manifold]
    connector: Core<'d, ID, LivenessApp, Static<Tcp>, common::Plain>,
}

/// Accepts `accepts` connections and holds each open and SILENT (never writes,
/// never closes) — earlier sockets stay alive while it blocks on the next
/// `accept`, so the client sees a silent peer, not an EOF. Reaching the 2nd
/// accept is proof the client force-reconnected.
fn silent_server(accepts: usize) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        let mut held: Vec<TcpStream> = Vec::with_capacity(accepts);
        for _ in 0..accepts {
            match listener.accept() {
                Ok((stream, _)) => held.push(stream),
                Err(_) => return,
            }
        }
        drop(held);
    });
    (addr, handle)
}

fn run(timeout: Option<Duration>, want: u32) -> (Rc<Gate>, Rc<Cell<Option<Duration>>>) {
    let (addr, server) = silent_server(2);
    let gate = Gate::new();
    let reconnect_at = Rc::new(Cell::new(None));

    let cfg = driver::Config::for_tcp_profile::<Throughput>(MAX);
    Executor::new(cfg).expect("executor").enter(|mut sess| {
        // `Core::with_app` resizes the dialer to `max_connections` itself.
        let seed = hash::Seed::new([1, 2]).state();
        let dialer = Static::<Tcp>::new(vec![addr], REDIAL_BACKOFF, seed);
        let connector = Core::with_app(
            LivenessApp {
                timeout,
                gate: gate.clone(),
                start: Instant::now(),
                reconnect_at: reconnect_at.clone(),
            },
            dialer,
            MAX,
            &mut sess.driver_access(),
        )
        .expect("connector");
        let app = pin!(BrandCell::new(App { connector }));
        common::run_until(&mut sess, app.as_ref(), &gate, want);
    });

    server.join().expect("server join");
    (gate, reconnect_at)
}

/// With the deadline set, a silent peer must be force-reconnected — and only
/// AFTER the deadline, proving the reconnect is liveness-driven (not a spurious
/// fast redial). `run_until(want=2)` blocks until the 2nd `connected()`; absent
/// the watchdog it would spin on the dead socket and fail at GUARD (5s).
#[test]
fn silent_peer_triggers_recoverable_reconnect() {
    let (_gate, reconnect_at) = run(Some(IDLE_TIMEOUT), 2);
    let dt = reconnect_at.get().expect("must reconnect after silence");
    assert!(
        dt >= IDLE_TIMEOUT - Duration::from_millis(30),
        "reconnect at {dt:?} — too early to be the idle deadline ({IDLE_TIMEOUT:?}); \
         a spurious redial, not liveness"
    );
    assert!(
        dt < IDLE_TIMEOUT * 8,
        "reconnect at {dt:?} — far past the deadline; watchdog is late"
    );
}

/// Control: with liveness OFF (`inbound_idle_timeout == None`), the same silent
/// peer must NOT trigger a reconnect. `run_until(want=2)` therefore times out at
/// GUARD with the gate stuck at 1 — the panic is caught and the single
/// connection asserted, pinning that reconnects come from the deadline alone.
#[test]
fn silent_peer_without_deadline_never_reconnects() {
    let outcome = std::panic::catch_unwind(|| run(None, 2));
    assert!(
        outcome.is_err(),
        "with no idle deadline a silent peer must never reconnect (gate cannot reach 2)"
    );
}
