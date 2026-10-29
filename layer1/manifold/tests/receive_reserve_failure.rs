pub mod common;

use std::{convert::Infallible, io::Write as _, ptr, time::Duration};

use common::{
    manifold::listener::trace::One,
    requests::{App, Event, State, Wire},
};
use dope_core::driver::{
    life::{creation::Attempt, domain::Error, recovery},
    route::{FrameworkTag, KeyTag, Tag, kind},
    settings::claim::Needs,
};
use dope_manifold::{
    app::{Host, env::Bundle, receive},
    network::listener::{self, topology::Layout},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::domain};
use dope_runtime::executor::{Executor, session};
use dope_test::{
    fibers,
    harness::{peer::Peer, signals::Progress as _},
    reclaim::Reports,
};

#[test]
fn batched_keeps_cause() {
    for retry in [false, true] {
        let config = One::config();
        let layout = Layout::<KeyTag<0>, receive::Borrowed>::new(config.connections);
        let driver = Needs::from_tree(&layout)
            .expect("listener layout")
            .resolve::<Throughput>()
            .expect("driver layout");
        let mut attempt = Attempt::new();
        let executor = {
            let result = Executor::new(&mut attempt, driver);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("native creation");
            result.ok().expect("native creation")
        };
        let mut reports = Reports::new(driver, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        executor.run(|executor| {
            let result = executor.enter_with(
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
                        |listener, scope| {
                            let addr = listener.local_addr();
                            let result = scope.with_quiesce(
                                &mut finish,
                                Host::new(listener),
                                Duration::from_secs(2),
                                |mut app| {
                                    let mut peer =
                                        Peer::at(addr).try_connect().expect("connect peer");
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
                    assert_eq!(
                        &*state.events.values().expect("receive events"),
                        &[Event::Wire(ptr::from_ref(&state.source), None)]
                    );
                    assert_eq!(state.reservations.get(), 1 + usize::from(retry));
                    assert_eq!(state.registered.get(), usize::from(retry));
                    assert_eq!(
                        state.cancelled.get(),
                        0,
                        "retried waits are already released"
                    );
                    assert_eq!(state.closed.hits(), 1);
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            result
                .as_ref()
                .map_err(Error::cause)
                .expect("finish receive domain");
        });
        reports.closed().expect("retired descriptors");
    }
}

#[test]
fn scoped_keeps_cause() {
    for retry in [false, true] {
        let config = One::config();
        let layout = Layout::<KeyTag<0>, receive::Scoped>::new(config.connections);
        let driver = Needs::from_tree(&layout)
            .expect("listener layout")
            .resolve::<Throughput>()
            .expect("driver layout");
        let mut attempt = Attempt::new();
        let executor = {
            let result = Executor::new(&mut attempt, driver);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("native creation");
            result.ok().expect("native creation")
        };
        let mut reports = Reports::new(driver, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        executor.run(|executor| {
            let result = executor.enter_with(
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
                        |listener, scope| {
                            let addr = listener.local_addr();
                            let result = scope.with_quiesce(
                                &mut finish,
                                Host::new(listener),
                                Duration::from_secs(2),
                                |mut app| {
                                    let mut peer =
                                        Peer::at(addr).try_connect().expect("connect peer");
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
                    assert_eq!(
                        &*state.events.values().expect("receive events"),
                        &[Event::Wire(ptr::from_ref(&state.source), None)]
                    );
                    assert_eq!(state.reservations.get(), 1 + usize::from(retry));
                    assert_eq!(state.registered.get(), usize::from(retry));
                    assert_eq!(
                        state.cancelled.get(),
                        0,
                        "retried waits are already released"
                    );
                    assert_eq!(state.closed.hits(), 1);
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            result
                .as_ref()
                .map_err(Error::cause)
                .expect("finish receive domain");
        });
        reports.closed().expect("retired descriptors");
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
