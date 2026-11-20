pub mod common;

use common::manifold::listener::finish::close::{script, wire::Wire};
use dope_core::driver::route::KeyTag;
use dope_manifold::{app::env::Bundle, network::listener::conn::deadline::Kind};
use dope_net::tcp::Tcp;
use dope_runtime::executor::session::Completion;
use dope_test::harness::{
    peer::Peer,
    signals::{Gate, Progress as _},
};

#[test]
fn wait_keeps_deadlines() {
    for draining in [false, true] {
        script::deadline(draining, false);
    }
}

#[test]
fn abandoned_queued_output_keeps_deadline() {
    script::deadline(false, true);
}

#[test]
fn shutdown_keeps_deadline() {
    use common::{manifold::listener::defer::wire::Profile, scenario::case::listener::Listener};
    use dope_test::fibers::Wait;

    let mut finish = Completion::new();
    let entered = Gate::new();
    let release = Gate::new();
    let received = Gate::new();
    let closed = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(1)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Profile>>((
            script::App {
                payload: None,
                close: closed.clone(),
                chunk: Some(received.clone()),
                timeout: Some(Kind::Send),
                abandon: false,
            },
            Wire::waiting(entered.clone(), release.clone()),
        ))
        .expect("listener layout");
    let (result, _reports) = prepared.observe(&mut finish, |case| {
        let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
        peer.write_all(b"REQ").expect("write request");
        assert_eq!(
            case.try_wait(&received, 1).expect("receive request"),
            Wait::Reached,
        );
        peer
    });
    assert_ne!(entered.hits(), 0, "shutdown reaches the held wire");
    assert_eq!(release.hits(), 0);
    let _peer = result.expect("shutdown keeps send deadline");
    assert_eq!(closed.hits(), 1);
}
