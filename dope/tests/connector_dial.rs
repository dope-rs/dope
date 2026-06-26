use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dope::manifold::connector::source::{Action, Dialer, Static};
use dope::transport::Tcp;

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

#[test]
fn empty_static_idles_until_dialed() {
    let mut d = Static::<Tcp>::new(vec![], Duration::from_millis(50));
    d.resize(4);
    let now = Instant::now();
    assert!(matches!(d.poll_connect(now), Action::Idle));

    let tag = d.dial(addr(9001)).expect("dial accepted");
    match d.poll_connect(now) {
        Action::Connect { tag: t, .. } => assert_eq!(t, tag),
        _ => panic!("expected Connect after dial"),
    }
    assert!(d.sock_addr(tag).is_some());
    assert!(matches!(d.poll_connect(now), Action::Idle));
}

#[test]
fn dialed_slot_is_one_shot_and_reused() {
    let mut d = Static::<Tcp>::new(vec![], Duration::from_millis(50));
    d.resize(2);
    let now = Instant::now();

    let tag = d.dial(addr(9002)).expect("dial");
    let _ = d.poll_connect(now);
    d.connect_outcome(tag, true, now);
    d.disconnect(tag, now);
    assert!(
        matches!(d.poll_connect(now), Action::Idle),
        "one-shot override slot must go dead on disconnect, not reconnect"
    );

    let tag2 = d.dial(addr(9003)).expect("redial");
    assert_eq!(tag, tag2, "spent override slot must be reused");
    assert!(d.sock_addr(tag2).is_some());
}

// Drains Connect actions exactly as Core::poll_source does: it only polls while
// the gate is open, and stops at the first non-Connect outcome.
fn settle(d: &mut Static<Tcp>, now: Instant) {
    while d.has_pending() {
        if !matches!(d.poll_connect(now), Action::Connect { .. }) {
            break;
        }
    }
}

#[test]
fn one_shot_dial_closes_the_gate_once_settled() {
    let mut d = Static::<Tcp>::new(vec![], Duration::from_millis(50));
    d.resize(4);
    let now = Instant::now();
    assert!(
        !d.has_pending(),
        "no upstreams and nothing dialed: gate shut"
    );

    let tag = d.dial(addr(9101)).expect("dial");
    assert!(d.has_pending(), "a queued dial opens the gate");
    match d.poll_connect(now) {
        Action::Connect { tag: t, .. } => assert_eq!(t, tag),
        _ => panic!("expected Connect"),
    }
    d.connect_outcome(tag, true, now);
    settle(&mut d, now);
    assert!(!d.has_pending(), "a connected slot leaves nothing to drive");

    d.disconnect(tag, now);
    settle(&mut d, now);
    assert!(
        !d.has_pending(),
        "spent one-shot slot is dead, gate stays shut"
    );
}

#[test]
fn failed_connect_keeps_gate_open_until_backoff_is_armed() {
    // Regression guard: a failed connect must leave the gate open so poll_source
    // runs once more and arms the backoff timer — otherwise the only upstream
    // never retries.
    let window = Duration::from_millis(20);
    let mut d = Static::<Tcp>::new(vec![addr(9301)], window);
    d.resize(1);
    let now = Instant::now();
    assert!(d.has_pending(), "static upstream is drivable at startup");

    let Action::Connect { tag, .. } = d.poll_connect(now) else {
        panic!("expected initial Connect");
    };
    d.connect_outcome(tag, false, now);
    assert!(
        d.has_pending(),
        "failed connect must keep the gate open to arm the backoff timer"
    );

    let retry_at = match d.poll_connect(now) {
        Action::Backoff { min_retry_at } => min_retry_at,
        _ => panic!("expected Backoff after failed connect"),
    };
    assert!(retry_at > now, "retry must be in the future");
    assert!(
        !d.has_pending(),
        "once Backoff is observed the armed timer owns the retry"
    );

    let due = retry_at + Duration::from_millis(1);
    match d.poll_connect(due) {
        Action::Connect { .. } => {}
        _ => panic!("backoff must retry once its deadline is due"),
    }
}

#[test]
fn revive_reopens_the_gate() {
    let mut d = Static::<Tcp>::new(vec![addr(9401)], Duration::from_millis(20));
    d.resize(1);
    let now = Instant::now();

    let Action::Connect { tag, .. } = d.poll_connect(now) else {
        panic!("expected Connect");
    };
    d.connect_outcome(tag, false, now);
    assert!(matches!(d.poll_connect(now), Action::Backoff { .. }));
    assert!(!d.has_pending(), "backed-off slot waits on the timer");

    d.revive();
    assert!(d.has_pending(), "revive makes the upstream drivable again");
    assert!(matches!(d.poll_connect(now), Action::Connect { .. }));
}
