pub mod common;

use std::{
    cell::Cell, convert::Infallible, env::consts, fmt::Debug, net::TcpStream, rc::Rc,
    time::Duration,
};

use common::{
    manifold::listener::{
        Cancel, CancelAt, Trace,
        probes::{Allocation, Opening, Wire},
        trace::{self, Active, App, Connect, One, Turns},
    },
    scenario::case::listener::Listener,
};
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    },
    io::socket::option,
};
use dope_manifold::{
    app::{
        Host,
        env::{Bundle, quota::PerIp},
    },
    network::listener,
    timing::Throughput,
};
use dope_net::{
    tcp::{StreamConfig, Tcp},
    wire::{Identity, domain},
};
use dope_runtime::{
    executor::{
        Executor,
        session::{self, Completion},
    },
    topology::error::Kind,
};
use dope_test::{
    fibers::{self, Run, Wait},
    harness::{
        observe,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn observe(event: trace::Event<'_, '_, KeyTag<0>>) {
    match event {
        trace::Event::Accept(state) => assert!(matches!(state, trace::Connection::Opening)),
        trace::Event::Close(state, current) => assert!(
            matches!(state, trace::Connection::Accepted(id) if *id == current),
            "only the original accepted connection may enter application close"
        ),
    }
}

fn native<R: Debug>(outcome: Result<R, Kind<'_, Infallible, &R>>) -> R {
    outcome.expect("native lifecycle")
}

fn reached(outcome: Result<Wait, Run>, actual: u32, want: u32) {
    assert_eq!(
        outcome.expect("drive progress"),
        Wait::Reached,
        "{actual} of {want} hits"
    );
}

#[test]
fn readiness_activates() {
    let mut finish = Completion::new();
    let hits = Rc::new(Cell::new(0));
    let gate = Gate::new();
    let prepared = Listener::new::<8>(Default::default())
        .per_ip(PerIp::UNLIMITED)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Active {
                hits: Rc::clone(&hits),
                gate: gate.clone(),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let peer = (Peer::at(case.addr()).try_connect()).expect("connect acceptance peer");
                reached(case.try_wait(&gate, 1), gate.hits(), 1);
                assert_eq!(hits.get(), 1);
                assert!(matches!(peer.peer_addr(), Ok(addr) if addr == case.addr()));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn accepts_once() {
    let mut finish = Completion::new();
    let trace = Rc::new(trace::Trace::default());
    let gate = Gate::new();
    let prepared = Listener::new::<64>(Default::default())
        .per_ip(PerIp::UNLIMITED)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            App {
                trace: trace.clone(),
                gate: gate.clone(),
                observe,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let peer = (Peer::at(case.addr()).try_connect()).expect("connect acceptance peer");
                reached(case.try_wait(&gate, 1), gate.hits(), 1);

                assert_eq!(trace.accepted.get(), 1, "accept must fire exactly once");
                assert!(matches!(peer.peer_addr(), Ok(addr) if addr == case.addr()));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn tuned_accepts() {
    let mut finish = Completion::new();
    let trace = Rc::new(trace::Trace::default());
    let gate = Gate::new();
    let stream = StreamConfig {
        no_delay: Some(option::Switch::On),
        ..Default::default()
    };
    let prepared = Listener::new::<64>(Default::default())
        .per_ip(PerIp::UNLIMITED)
        .stream(stream)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            App {
                trace: trace.clone(),
                gate: gate.clone(),
                observe,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let peer = (Peer::at(case.addr()).try_connect()).expect("connect acceptance peer");
                reached(case.try_wait(&gate, 1), gate.hits(), 1);

                assert_eq!(trace.accepted.get(), 1, "tuned accept must publish once");
                assert!(matches!(peer.peer_addr(), Ok(addr) if addr == case.addr()));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn shutdown_cancels_tuning() {
    let mut finish = Completion::new();
    assert_eq!(consts::OS, "linux", "tuning cancellation requires Linux");
    let app = Rc::new(trace::Trace::default());
    let disp = Rc::new(Trace::default());
    let tuning = Gate::new();
    let config = One::driver();
    let config = config.expect("resolve listener cancellation driver settings");
    use dope_core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, config);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("create executor");
    let executor = executor.ok().expect("executor");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
        let result =
            executor.enter_with(
                |mut sess| {
                    let hash = sess.hash_state(listener::Domain::DEFAULT);

                    let result =
                        listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                            One::config(),
                            domain::Local::new(Identity),
                        )
                        .with_driver(
                            App {
                                trace: Rc::clone(&app),
                                gate: Gate::new(),
                                observe,
                            },
                            hash,
                            &mut sess,
                            session::Scope::driver,
                            |server, sess| {
                                let addr = server.local_addr();
                                let result = sess.with_quiesce(&mut finish,
                        Host::new(Cancel {
                            listener: server,
                            at: CancelAt::Tuning,
                            trace: Rc::clone(&disp),
                            gate: tuning.clone(),
                        }),
                        Duration::from_secs(2),
                        |mut host| {
                            let peer =
                                (TcpStream::connect(addr)).expect("connect cancellation peer");
                            reached(fibers::TEST.try_wait(&mut host, &tuning, 1), tuning.hits(), 1);
                            assert!(matches!(peer.peer_addr(), Ok(observed) if observed == addr));
                        },
                    );
                                assert!(result.is_ok(), "run cancellation test application");
                            },
                        );

                    assert!(result.is_ok(), "open cancellation listener");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
        assert!(
            reports.events().expect("reclaimed events").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "finish cancellation executor domain");
        assert_eq!(disp.accepts(), 1);
        assert_eq!(disp.tunings(), 1);
        assert!((disp.tuned()) == (Some(o3::depot::capacity::Count::EMPTY)));
        assert_eq!(app.accepted.get(), 0);
    });
}

#[test]
fn resource_exhaustion_recovers() {
    use common::manifold::listener::defer::script::Until;
    use dope_core::{
        driver::{
            life::creation::Attempt,
            route::table::conn::Capacity,
            settings::{Resource as _, claim::Needs},
        },
        io::Reclaimed,
    };
    use dope_manifold::{app::receive, network::listener::topology::Layout};

    let layout = Layout::<KeyTag<0>, receive::Borrowed>::new(Capacity::fixed::<1>());
    let config = layout
        .include_in(Needs::new())
        .expect("listener resources")
        .resolve::<Throughput>()
        .expect("listener settings");
    let mut creation = Attempt::new();
    let executor = Executor::new(&mut creation, config);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor");
    let executor = executor.ok().expect("executor");
    let mut finish = Completion::new();
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let retained = Gate::new();
    let failures = Gate::new();
    let accepted = Gate::new();
    let app = Rc::new(trace::Trace::default());

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let hash = session.hash_state(listener::Domain::DEFAULT);
                let mut config = One::config();
                config.stream = StreamConfig::default();
                listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                    config,
                    domain::Local::new(Identity),
                )
                .with_driver(
                    App {
                        trace: app.clone(),
                        gate: accepted.clone(),
                        observe,
                    },
                    hash,
                    &mut session,
                    session::Scope::driver,
                    |server, session| {
                        let addr = server.local_addr();
                        let start = session.driver().driver_ref().clock().now().into();
                        session
                            .with_at_for(
                                &mut finish,
                                Host::new(Cancel {
                                    listener: server,
                                    at: CancelAt::Retain(Cell::new(None)),
                                    trace: Rc::new(Trace::default()),
                                    gate: retained.clone(),
                                }),
                                start,
                                Duration::from_secs(2),
                                |mut host| {
                                    let mut peers =
                                        vec![Peer::at(addr).try_connect().expect("retained peer")];
                                    host.drive_for(
                                        start,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &retained,
                                            hits: 1,
                                        },
                                    )
                                    .expect("retain accepted slot")
                                    .into_output();
                                    peers.push(Peer::at(addr).try_connect().expect("waiting peer"));
                                    host.drive_for(
                                        start,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &failures,
                                            hits: 1,
                                        },
                                    )
                                    .expect("native resource failure")
                                    .into_output();
                                    assert_eq!(accepted.hits(), 0);
                                    assert_eq!(failures.hits(), 1);
                                    // Native slot installation follows accept, so each
                                    // rejection closes that socket. Supply the next peer.
                                    peers.push(Peer::at(addr).try_connect().expect("retry peer"));
                                    let before = start
                                        .try_add(Duration::from_millis(9))
                                        .expect("before retry");
                                    host.drive_for(
                                        before,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &failures,
                                            hits: 1,
                                        },
                                    )
                                    .expect("cooldown fixed point")
                                    .into_output();
                                    assert_eq!(
                                        failures.hits(),
                                        1,
                                        "no accept retry before deadline"
                                    );
                                    let due = start
                                        .try_add(Duration::from_millis(10))
                                        .expect("retry deadline");
                                    host.drive_for(
                                        due,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &failures,
                                            hits: 2,
                                        },
                                    )
                                    .expect("bounded resource retry")
                                    .into_output();
                                    assert_eq!(failures.hits(), 2);
                                    {
                                        let slot = host.inspect(
                                            |host| host.manifold(),
                                            |host| match &host.get_ref().at {
                                                CancelAt::Retain(slot) => slot.take(),
                                                _ => None,
                                            },
                                        );
                                        assert!(slot.is_some(), "release the exact accepted slot");
                                    }
                                    peers
                                        .push(Peer::at(addr).try_connect().expect("recovery peer"));
                                    let due = start
                                        .try_add(Duration::from_millis(20))
                                        .expect("recovery deadline");
                                    host.drive_for(
                                        due,
                                        Duration::from_secs(1),
                                        Until {
                                            gate: &accepted,
                                            hits: 1,
                                        },
                                    )
                                    .expect("same listener recovers")
                                    .into_output();
                                    assert_eq!(failures.hits(), 2);
                                    assert_eq!(accepted.hits(), 1);
                                },
                            )
                            .expect("listener retirement");
                    },
                )
                .expect("listener installation");
            },
            recovery::Policy::new(
                |event| {
                    if let Reclaimed::AcceptRoute { cause, .. } = &event {
                        assert!(cause.is_resource());
                        failures.hit();
                    }
                    reports.push(event);
                },
                recover,
            ),
        );
        assert!(result.is_ok(), "listener runtime");
    });
    assert_eq!(reports.events().expect("reclaimed events").len(), 2);
}

#[test]
fn ip_release_keeps_peers() {
    let mut finish = Completion::new();
    let trace = Rc::new(trace::Trace::default());
    let gate = Gate::new();
    let prepared = Listener::new::<8>(Default::default())
        .per_ip(PerIp::fixed::<2>())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            App {
                trace: trace.clone(),
                gate: gate.clone(),
                observe,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peers = case.addr().connect_n(2).expect("fill IP quota");
                reached(case.try_wait(&gate, 2), gate.hits(), 2);
                peers.truncate(1);
                reached(case.try_wait(&trace.closed, 1), trace.closed.hits(), 1);
                peers.push(
                    Peer::at(case.addr())
                        .try_connect()
                        .expect("replace one peer"),
                );
                reached(case.try_wait(&gate, 3), gate.hits(), 3);
                peers.push(
                    Peer::at(case.addr())
                        .try_connect()
                        .expect("excess peer socket"),
                );
                assert!(case.try_drive(Turns(64)).is_ok());
                assert_eq!(
                    trace.accepted.get(),
                    3,
                    "the surviving peer still occupies quota"
                );
                peers.clear();
                reached(case.try_wait(&trace.closed, 3), trace.closed.hits(), 3);
                let mut peers = case
                    .addr()
                    .connect_n(2)
                    .expect("reuse the retired IP entry");
                reached(case.try_wait(&gate, 5), gate.hits(), 5);
                assert_eq!(
                    trace.accepted.get(),
                    5,
                    "last release retires the exact IP entry"
                );
                peers.clear();
                reached(case.try_wait(&trace.closed, 5), trace.closed.hits(), 5);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn admission_ignores_timing() {
    let mut finish = Completion::new();
    let trace = Rc::new(trace::Trace::default());
    let gate = Gate::new();
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput, Throughput, One>>((
            App {
                trace: trace.clone(),
                gate: gate.clone(),
                observe,
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peers = (case.addr().connect_n(2)).expect("connect quota peers");
                reached(case.try_wait(&gate, 1), gate.hits(), 1);
                assert!(case.try_drive(Turns(64)).is_ok());
                assert_eq!(trace.accepted.get(), 1);
                peers.clear();
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn wire_failure_releases_ip() {
    let mut finish = Completion::new();
    let failed = Gate::new();
    let accepted = Rc::new(Cell::new(0));
    let gate = Gate::new();
    let prepared = Listener::new::<8>(Default::default())
        .per_ip(PerIp::fixed::<1>())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Opening {
                accepted: accepted.clone(),
                gate: gate.clone(),
                failed: failed.clone(),
            },
            (),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                {
                    let addr = case.addr();
                    let first =
                        (Peer::at(addr).try_connect()).expect("connect first admission peer");
                    reached(case.try_wait(&failed, 1), failed.hits(), 1);
                    let second =
                        (Peer::at(addr).try_connect()).expect("connect second admission peer");
                    reached(case.try_wait(&gate, 1), gate.hits(), 1);
                    assert!(matches!(first.peer_addr(), Ok(observed) if observed == addr));
                    assert!(matches!(second.peer_addr(), Ok(observed) if observed == addr));
                }

                assert_eq!(accepted.get(), 1);
                assert_eq!(failed.hits(), 1);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn alloc_error_releases_ip() {
    let mut finish = Completion::new();
    let accepted = Rc::new(Cell::new(0));
    let gate = Gate::new();
    let failed = Gate::new();
    let closed = Gate::new();
    let causes = Rc::new(observe::Trace::new());
    let prepared = Listener::new::<8>(Default::default())
        .per_ip(PerIp::fixed::<1>())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Allocation {
                reject: Cell::new(true),
                accepted: accepted.clone(),
                gate: gate.clone(),
                failed: failed.clone(),
                closed: closed.clone(),
                causes: Rc::clone(&causes),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                {
                    let addr = case.addr();
                    let first =
                        (Peer::at(addr).try_connect()).expect("connect first admission peer");
                    reached(case.try_wait(&failed, 1), failed.hits(), 1);
                    let second =
                        (Peer::at(addr).try_connect()).expect("connect second admission peer");
                    reached(case.try_wait(&gate, 1), gate.hits(), 1);
                    reached(case.try_wait(&closed, 1), closed.hits(), 1);
                    assert!(matches!(first.peer_addr(), Ok(observed) if observed == addr));
                    assert!(matches!(second.peer_addr(), Ok(observed) if observed == addr));
                }

                assert_eq!(accepted.get(), 1);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert_eq!(
        &*causes.values().expect("allocation error observations"),
        &[o3::alloc::Error::Overflow]
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
