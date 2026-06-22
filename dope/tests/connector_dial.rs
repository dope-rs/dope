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
