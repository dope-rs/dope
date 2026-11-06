pub mod common;

use std::time::Duration;

use common::{
    manifold::listener::trace::One,
    recover,
    requests::{App, State, Wire},
    scenario::Runtime,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{Host, env::Bundle, receive},
    network::listener::{self, topology::Layout},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::domain};
use dope_runtime::executor::session;
use dope_test::{fibers, harness::peer::Peer, reclaim::Reports};

#[test]
fn batched_keeps_cause() {
    for retry in [false, true] {
        let config = One::config();
        let layout = Layout::<KeyTag<0>, receive::Borrowed>::new(config.connections);
        let runtime = Runtime::try_for_plan::<Throughput, _>(&layout).expect("driver layout");
        let mut reports = Reports::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        runtime.with_scope(
            |mut scope| {
                let state = State::new(retry);
                let mut finish = session::Completion::new();
                let hash = scope.hash_state(listener::Domain::DEFAULT);
                listener::Install::<KeyTag<0>, Bundle<Tcp, Wire, Throughput>, _>::new(
                    config,
                    domain::Local::new(&state),
                )
                .with_driver(
                    App::<receive::Borrowed>::new(&state),
                    hash,
                    &mut scope,
                    session::Scope::driver,
                    |listener, _wait, scope| {
                        let addr = listener.local_addr();
                        let result = scope.with_quiesce(
                            &mut finish,
                            Host::new(listener),
                            Duration::from_secs(2),
                            |mut app| {
                                let peer = Peer::at(addr).try_connect().expect("connect peer");
                                peer.write_all(b"failure").expect("write receive");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &state.closed, 1)
                                        .expect("drive failed receive"),
                                    fibers::Wait::Reached,
                                    "receive close deadline"
                                );
                            },
                        );
                        assert!(result.is_ok(), "finish failed receive");
                    },
                )
                .expect("install listener");
                state.check();
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        reports.closed().expect("retired descriptors");
    }
}

#[test]
fn scoped_keeps_cause() {
    for retry in [false, true] {
        let config = One::config();
        let layout = Layout::<KeyTag<0>, receive::Scoped>::new(config.connections);
        let runtime = Runtime::try_for_plan::<Throughput, _>(&layout).expect("driver layout");
        let mut reports = Reports::new(runtime.config(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        runtime.with_scope(
            |mut scope| {
                let state = State::new(retry);
                let mut finish = session::Completion::new();
                let hash = scope.hash_state(listener::Domain::DEFAULT);
                listener::Install::<KeyTag<0>, Bundle<Tcp, Wire, Throughput>, _>::new(
                    config,
                    domain::Local::new(&state),
                )
                .with_driver(
                    App::<receive::Scoped>::new(&state),
                    hash,
                    &mut scope,
                    session::Scope::driver,
                    |listener, _wait, scope| {
                        let addr = listener.local_addr();
                        let result = scope.with_quiesce(
                            &mut finish,
                            Host::new(listener),
                            Duration::from_secs(2),
                            |mut app| {
                                let peer = Peer::at(addr).try_connect().expect("connect peer");
                                peer.write_all(b"failure").expect("write receive");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &state.closed, 1)
                                        .expect("drive failed receive"),
                                    fibers::Wait::Reached,
                                    "receive close deadline"
                                );
                            },
                        );
                        assert!(result.is_ok(), "finish failed receive");
                    },
                )
                .expect("install listener");
                state.check();
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        reports.closed().expect("retired descriptors");
    }
}
