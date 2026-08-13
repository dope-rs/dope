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
use std::{
    cell::Cell,
    convert::Infallible,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::driver::settings;
use dope_manifold::{
    Bundle,
    connector::{
        codec::{Codec, Parse},
        lifecycle::{CloseReason, Stateless},
        session::{Ctx, Retirement, Scheduling, Session, Target},
    },
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, peer::Peer, scenario::scenarios::Connector};
use o3::buffer::storage::Shared;

// Exactly one upstream connection, so `connected()` fires once per real dial —
// the reconnect is unambiguous. (With N slots the connector eagerly opens N
// connections to the single addr at startup, which would satisfy the gate
// without ever exercising liveness.)
const MAX: usize = 1;
const IDLE_TIMEOUT: Duration = Duration::from_millis(150);
const REDIAL_BACKOFF: Duration = Duration::from_millis(20);

struct NeedMore;

impl Codec for NeedMore {
    type Head<'input, 'd> = ();
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        _buf: dope_manifold::connector::codec::Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        Ok(Parse::NeedMore)
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

/// Minimal service session that opts into liveness and records established connections.
struct LivenessApp {
    codec: NeedMore,
    timeout: Option<Duration>,
    expected_peer: std::net::SocketAddr,
    gate: Gate,
    start: Instant,
    /// Wall-clock offset of the 2nd `connected()` — i.e. when the reconnect
    /// completed. Used to prove the reconnect was driven by the idle deadline
    /// (≈ `IDLE_TIMEOUT`) and not some spurious fast redial.
    reconnect_at: Rc<Cell<Option<Duration>>>,
}

impl<'d> Session<'d> for LivenessApp {
    type Codec = NeedMore;
    type ConnState = Stateless;
    type Send = Shared;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn connect(&mut self, peer: dope_core::io::socket::Addr, _context: &mut Ctx<'_, 'd, Self>) {
        assert_eq!(peer.into_std().unwrap(), self.expected_peer);
        self.gate.hit();
        if self.gate.hits() == 2 {
            self.reconnect_at.set(Some(self.start.elapsed()));
        }
    }

    fn response<'input>(&mut self, _head: (), _context: &mut Ctx<'_, 'd, Self>)
    where
        'd: 'input,
    {
    }
}

impl<'d> Retirement<'d> for LivenessApp {
    fn disconnect(&mut self, _context: &mut Ctx<'_, 'd, Self>, _reason: CloseReason) {}
}

impl<'d> Scheduling<'d> for LivenessApp {
    fn inbound(
        &self,
        _connection: dope_manifold::connector::connection::Id<'d, 0>,
        _state: &Self::ConnState,
        _default: dope_manifold::timing::Window,
        _region: &mut o3::cell::region::Token<'d>,
    ) -> dope_manifold::connector::app::Inbound {
        self.timeout
            .and_then(dope_manifold::timing::Window::new)
            .map_or(
                dope_manifold::connector::app::Inbound::Quiescent,
                dope_manifold::connector::app::Inbound::Awaiting,
            )
    }
}

impl<'d> Target<'d, 0, MAX> for LivenessApp {}

fn run(timeout: Option<Duration>, want: u32) -> (bool, Gate, Rc<Cell<Option<Duration>>>) {
    let (addr, server) = Peer::hold(2);
    let gate = Gate::new();
    let reconnect_at = Rc::new(Cell::new(None));

    let reached = Connector::<MAX>::new(addr, REDIAL_BACKOFF)
        .timer_cache_limit(settings::ScheduleCapacity::ZERO)
        .run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
            LivenessApp {
                codec: NeedMore,
                timeout,
                expected_peer: addr,
                gate: gate.clone(),
                start: Instant::now(),
                reconnect_at: reconnect_at.clone(),
            },
            |case| case.wait_until(&gate, want),
        );

    if !reached {
        let _ = std::net::TcpStream::connect(addr);
    }
    server.join().expect("server join");
    (reached, gate, reconnect_at)
}

/// With the deadline set, a silent peer must be force-reconnected — and only
/// AFTER the deadline, proving the reconnect is liveness-driven (not a spurious
/// fast redial). The bounded wait reaches the 2nd `connected()` only when the
/// watchdog forces a reconnect.
#[test]
fn silent_peer_triggers_recoverable_reconnect() {
    let (reached, _gate, reconnect_at) = run(Some(IDLE_TIMEOUT), 2);
    assert!(reached, "idle deadline must reconnect the silent peer");
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

/// Control: when the connection reports `Inbound::Quiescent`, the same silent
/// peer must NOT trigger a reconnect. The bounded wait must expire with the gate
/// stuck at one, pinning that reconnects come from the deadline alone.
#[test]
fn silent_peer_without_deadline_never_reconnects() {
    let (reached, gate, _) = run(None, 2);
    assert!(!reached, "a quiescent connection must not redial");
    assert_eq!(gate.hits(), 1);
}
