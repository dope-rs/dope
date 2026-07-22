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

extern crate dope;

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use dope::DriverContext;
use dope::manifold::connector::source::DialKey;
use dope::manifold::connector::state::State;
use dope::manifold::connector::{ChunkOutcome, ConnApp};
use dope_net::wire::identity::Identity;
use dope_test::{Gate, hold_connections};
use o3::buffer::{RetainBytes, Shared};

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

fn run(timeout: Option<Duration>, want: u32) -> (Rc<Gate>, Rc<Cell<Option<Duration>>>) {
    let (addr, server) = hold_connections(2);
    let gate = Gate::new();
    let reconnect_at = Rc::new(Cell::new(None));

    dope_test::connector_case! {
        max_connections: MAX,
        address: addr,
        backoff: REDIAL_BACKOFF,
        app: LivenessApp {
            timeout,
            gate: gate.clone(),
            start: Instant::now(),
            reconnect_at: reconnect_at.clone(),
        },
        |case| {
            case.until(&gate, want);
        }
    }

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
