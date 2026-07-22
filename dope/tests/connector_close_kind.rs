//! Anchor test for app-initiated close intent (`CloseKind`).
//!
//! The receive path can already ask for a reconnecting vs a permanent close
//! (`ChunkOutcome::CloseReconnect` / `ClosePermanent`). This pins the SYMMETRIC
//! guarantee for a close an app requests OFF the receive path (a supervisor /
//! timer, via `ConnApp::drain_requests`): `CloseKind::Reconnect` must redial,
//! `CloseKind::Permanent` must NOT (else a terminal fault redials into the same
//! doomed connection forever). Real reactor + real loopback: no unit tests.
//!
//! Lives in `dope/tests` (the layer that owns the connector) so a refactor keeps
//! it green.
#![cfg(target_os = "linux")]

extern crate dope;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use dope::DriverContext;
use dope::driver::token::Token;
use dope::manifold::connector::source::DialKey;
use dope::manifold::connector::state::State;
use dope::manifold::connector::{ChunkOutcome, CloseKind, ConnApp, Requests};
use dope_net::wire::identity::Identity;
use dope_test::{Gate, hold_connections};
use o3::buffer::{RetainBytes, Shared};

const MAX: usize = 1;
const REDIAL_BACKOFF: Duration = Duration::from_millis(20);

type Slot<'d> = dope_net::link::slot::Slot<'d, Identity, State<(), Shared>>;

/// On the first established connection, asks the connector to close it with a
/// configured `CloseKind` (via `drain_requests`, i.e. off the receive path).
struct CloseKindApp {
    kind: CloseKind,
    gate: Rc<Gate>,
    // Interior-mutable: `drain_requests` is `&self`. Armed once on connect #1.
    pending: Cell<Option<Token>>,
}

impl<'d> ConnApp<'d> for CloseKindApp {
    type Conn = ();
    type Wire = Identity;
    type Send = Shared;

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
        slot: &mut Slot<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.gate.hit();
        if self.gate.hits() == 1 {
            // Arm an app-initiated close of THIS connection and kick an activation
            // so the connector drains our request on the next turn.
            self.pending.set(Some(slot.token()));
            driver.driver_ref().activate_ready(slot.ready_key());
        }
    }

    fn drain_requests(
        &self,
        token: Token,
        _push: impl FnMut(Self::Send) -> Result<(), Self::Send>,
    ) -> Requests {
        if self.pending.get() == Some(token) {
            self.pending.set(None);
            Requests {
                shutdown: None,
                close: Some(self.kind),
            }
        } else {
            Requests::default()
        }
    }

    fn send(&mut self, _slot: &mut Slot<'d>, _sent: usize, _driver: &mut DriverContext<'_, 'd>) {}

    fn close(&mut self, _slot: &mut Slot<'d>) {}
}

fn run(kind: CloseKind, want: u32) -> Rc<Gate> {
    let (addr, srv) = hold_connections(2);
    let gate = Gate::new();

    dope_test::connector_case! {
        max_connections: MAX,
        address: addr,
        backoff: REDIAL_BACKOFF,
        app: CloseKindApp {
            kind,
            gate: gate.clone(),
            pending: Cell::new(None),
        },
        |case| {
            case.until(&gate, want);
        }
    }

    srv.join().expect("server join");
    gate
}

/// `CloseKind::Reconnect` drops the socket and redials → a 2nd `connected()`.
#[test]
fn app_reconnect_close_redials() {
    let gate = run(CloseKind::Reconnect, 2);
    assert_eq!(
        gate.hits(),
        2,
        "reconnect close must redial to a 2nd connection"
    );
}

/// `CloseKind::Permanent` drops the socket and does NOT redial. `run_until`
/// (want=2) therefore times out at GUARD with the gate stuck at 1 — the panic is
/// caught and the single connection asserted.
#[test]
fn app_permanent_close_does_not_redial() {
    let outcome = std::panic::catch_unwind(|| run(CloseKind::Permanent, 2));
    assert!(
        outcome.is_err(),
        "permanent close must NOT redial (gate cannot reach 2)"
    );
}
