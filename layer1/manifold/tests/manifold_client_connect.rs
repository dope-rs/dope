pub mod common;

use std::{cell::Cell, fmt::Debug, net::SocketAddr, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag, table::conn::Capacity},
        settings,
    },
    io::socket,
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::{attempt::StreamTarget, conn::engine},
    network::client::topology::{Install, plan},
    service::{Endpoint, Revision, Snapshot, sync},
    timing::Throughput,
};
use dope_net::{
    link::event::{AttemptError, ConnectFault},
    tcp::Tcp,
    wire::{Identity, domain::Local},
};
use dope_runtime::{
    executor::{
        drive,
        session::{Completion, Scope},
    },
    topology::{self, error::Kind, lending},
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer::{self, TcpAcceptor},
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{
    depot::{arena::slab::key::Observation, capacity},
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        connect::{
            dial::{
                Exhausted,
                empty::{self, Empty},
            },
            open::{
                Counts, DeferredWire, Failed, Runtime as OpenRuntime, Wire,
                deferred::Deferred,
                parked::{self, Parked},
            },
            rejected::Rejected,
            resolve::{Addr, App, Net, Wait},
        },
        error,
        finish::close::members::Until,
    },
    recover,
    scenario::{
        Runtime,
        case::attempt::{Attempt, Attempts},
    },
};

#[test]
fn reject_skips_callbacks() {
    let mut finish = Completion::new();
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind peer");
    let addr = server.local_addr().expect("peer address");
    let gate = Gate::new();
    let scoped = Rc::new(Cell::new(0));
    let audit = Rc::new(Report::default());

    let planned =
        Attempt::new(addr).prepare::<KeyTag<3>, _, Bundle<Tcp, Identity, Throughput>>(Rejected {
            audit: Rc::clone(&audit),
            gate: gate.clone(),
            scoped: Rc::clone(&scoped),
        });
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<3>()]);

    let run = planned.with_run(
        &mut finish,
        Identity,
        |case| reached(case.try_wait(&gate, 1), &audit),
        recovery::Policy::new(|event| reclaim1.push(event), recover),
        leased,
    );
    run.expect("rejected attempt scenario failed");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    assert_eq!(scoped.get(), 0);
    audit.checked((0, 0));
}

#[test]
fn reports_state_failure() {
    let mut finish = Completion::new();
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind rejection peer");
    let done = Gate::new();
    let trace = Rc::new(empty::Trace::default());
    let audit = Rc::new(Report::default());

    let planned = Attempt::new(server.local_addr().expect("peer address"))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Empty {
            audit: Rc::clone(&audit),
            trace: Rc::clone(&trace),
            reject: Some(done.clone()),
        });
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            Identity,
            |case| reached(case.try_wait(&done, 1), &audit),
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            leased,
        )
        .expect("connection state rejection must retire its transport");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );
    assert!(matches!(
        trace.failed.values().as_deref(),
        Ok([ConnectFault::Transport(
            dope_net::link::event::ConnectError::State(rc::Error::Exhausted)
        )])
    ));
    assert_eq!(trace.prepared.get(), 0);
    assert_eq!(trace.admitted.get(), 0);
    audit.checked((0, 0));
    assert_eq!(trace.connected.get(), 0);
    assert_eq!(trace.closed.get(), 0);
    assert_eq!(trace.drained.get(), 0);
    let stream = server
        .accept_for(GUARD)
        .expect("accept rejection peer")
        .expect("connected peer");
    stream.read_wait(Some(GUARD)).expect("peer read deadline");
    assert_eq!(
        stream.read(&mut [0]).expect("rejected connection closes"),
        0
    );
}

#[test]
fn fatal_opens_report_once_and_preserve_progress() {
    for count in [1, 16] {
        let mut finish = Completion::new();
        let mut create = dope_core::driver::life::creation::Attempt::new();
        let runtime = Runtime::throughput();
        let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
        let gate = Gate::new();
        let audit = Rc::new(Report::default());
        let counts = Rc::new(Counts::default());
        let executor = runtime.try_executor(&mut create).ok().expect("executor");
        executor
            .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(count)))
            .run(|executor| {
                let result = executor.try_create(
                    |mut session| {
                        let source = session.storage();
                        let attempts = (0..count)
                            .map(|_| {
                                source.try_reserve().expect("queued attempt").start(
                                    StreamTarget::new(
                                        SocketAddr::from(([127, 0, 0, 1], 9)),
                                        Default::default(),
                                    ),
                                )
                            })
                            .collect::<Vec<_>>();
                        engine::Startup::new(
                            Failed {
                                audit: audit.clone(),
                                counts: Rc::clone(&counts),
                                gate: gate.clone(),
                            },
                            source,
                            Capacity::fixed::<1>(),
                            Default::default(),
                            Local::new(OpenRuntime(None)),
                        )
                        .try_with::<Bundle<Tcp, Wire, Throughput>, _, _>(
                            &mut session,
                            Scope::driver,
                            |engine, session| {
                                session
                                    .with_quiesce(
                                        &mut finish,
                                        Host::new(engine),
                                        Duration::from_secs(2),
                                        |app| {
                                            assert!(matches!(
                                                app.run_for(Duration::from_millis(100)),
                                                Err(drive::Error::Timeout)
                                            ));
                                            assert_eq!(
                                                gate.hits(),
                                                count,
                                                "all queued opens complete"
                                            );
                                        },
                                    )
                                    .expect("application teardown");
                            },
                        )
                        .expect("connector installation");
                        assert!(!source.has_room(), "failed attempts retain their leases");
                        for attempt in attempts {
                            attempt.cancel();
                        }
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                );
                result.as_ref().map_err(Kind::from).expect("attempt source");
            });
        assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
        assert_eq!(gate.hits(), count, "each failed open reports once");
        assert_eq!(
            [
                counts.received.get(),
                counts.connected.get(),
                counts.prepared.get(),
                counts.closed.get(),
                counts.drained.get(),
            ],
            [0; 5],
        );
        audit.checked((0, 0));
    }
}

#[test]
fn fresh_open_survives_shared_maintenance() {
    let mut finish = Completion::new();
    let mut create = dope_core::driver::life::creation::Attempt::new();
    let runtime = Runtime::throughput();
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    let gate = Gate::new();
    let audit = Rc::new(Report::default());
    let exhaust = Cell::new(false);
    let executor = runtime.try_executor(&mut create).ok().expect("executor");
    executor
        .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
        .run(|executor| {
            let result = executor.try_create(
                |mut session| {
                    let source = session.storage();
                    engine::Startup::new(
                        Failed {
                            audit: audit.clone(),
                            counts: Rc::new(Counts::default()),
                            gate: gate.clone(),
                        },
                        source,
                        Capacity::fixed::<1>(),
                        Default::default(),
                        Local::new(OpenRuntime(None)),
                    )
                    .try_with::<Bundle<Tcp, Wire, Throughput>, _, _>(
                        &mut session,
                        Scope::driver,
                        |engine, session| {
                            session
                                .with_quiesce(
                                    &mut finish,
                                    Host::new(Exhausted {
                                        inner: engine,
                                        exhaust: &exhaust,
                                    }),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        fibers::TEST
                                            .try_pause(&mut app, Duration::from_millis(100))
                                            .expect("quiet empty engine");
                                        let attempt = source
                                            .try_reserve()
                                            .expect("fresh attempt")
                                            .start(StreamTarget::new(
                                                SocketAddr::from(([127, 0, 0, 1], 9)),
                                                Default::default(),
                                            ));
                                        exhaust.set(true);
                                        assert!(matches!(
                                            app.run_for(Duration::from_millis(100)),
                                            Err(drive::Error::Timeout)
                                        ));
                                        assert!(!exhaust.get());
                                        assert_eq!(gate.hits(), 1, "fresh opening must run");
                                        attempt.cancel();
                                    },
                                )
                                .expect("application teardown");
                        },
                    )
                    .expect("connector installation");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            result.as_ref().map_err(Kind::from).expect("attempt source");
        });
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    audit.checked((0, 0));
}

#[test]
fn cancelled_open_preserves_reused_attempt() {
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind replacement peer");
    let addr = server.local_addr().expect("peer address");
    let mut finish = Completion::new();
    let mut create = dope_core::driver::life::creation::Attempt::new();
    let runtime = Runtime::throughput();
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    let audit = Rc::new(Report::default());
    let counts = Rc::new(Counts::default());
    let executor = runtime.try_executor(&mut create).ok().expect("executor");
    executor
                .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
                .run(|executor| {
                    let result = executor.try_create(
                        |mut session| {
                            let source = session.storage();
                            let mut current = Some(
                                source.try_reserve().expect("first attempt").start(
                                    StreamTarget::new(([127, 0, 0, 1], 0).into(), Default::default()),
                                ),
                            );
                            let old = current.as_ref().expect("first owner").id();
                            let replacement = Cell::new(None);
                            let next = Cell::new(None);
                            let mut replace = || {
                                let Some(current) = current.take() else {
                                    return false;
                                };
                                current.cancel();
                                let lease = source.try_reserve().expect("same physical slot").start(
                                    StreamTarget::new(addr, Default::default()),
                                );
                                assert_eq!(lease.id().index(), old.index());
                                assert_ne!(lease.id().generation(), old.generation());
                                next.set(Some(lease.id()));
                                replacement.set(Some(lease));
                                true
                            };
                            engine::Startup::new(
                                Failed {
                                    audit: audit.clone(),
                                    counts: counts.clone(),
                                    gate: Gate::new(),
                                },
                                source,
                                Capacity::fixed::<1>(),
                                Default::default(),
                                Local::new(OpenRuntime(Some(&mut replace))),
                            )
                            .try_with::<Bundle<Tcp, Wire, Throughput>, _, _>(
                                &mut session,
                                Scope::driver,
                                |engine, session| {
                                    let now = session.driver().driver_ref().clock().now();
                                    session.with_at_for(
                                        &mut finish,
                                        Host::new(engine),
                                        now.into(),
                                        Duration::from_secs(2),
                                        |mut app| {
                                            app.drive_for(
                                                now.into(),
                                                Duration::from_secs(2),
                                                Until(|| counts.failed.get().is_some()),
                                            )
                                            .expect("cancel during wire open")
                                            .into_output();
                                            let next = next.get().expect("replacement published");
                                            assert_eq!(
                                                counts.failed.get(),
                                                Some((old.index().get(), old.generation()))
                                            );
                                            assert!(matches!(
                                                audit.faults.values().as_deref(),
                                                Ok([error::Error::Connect(ConnectFault::Attempt(
                                                    AttemptError::Lookup(Observation::Generation { actual })
                                                ))]) if *actual == next.generation()
                                            ));
                                            assert_eq!(
                                                [counts.received.get(), counts.connected.get(), counts.prepared.get(), counts.closed.get(), counts.drained.get()],
                                                [0; 5]
                                            );
                                            assert!(!source.has_room());
                                            app.drive_for(
                                                now.clamp_add(Duration::from_millis(2)),
                                                Duration::from_secs(2),
                                                Until(|| counts.connected.get() == 1),
                                            )
                                            .expect("replacement target connects")
                                            .into_output();
                                            replacement.take().expect("replacement owner").cancel();
                                        },
                                    )
                                    .expect("application teardown");
                                },
                            )
                            .expect("connector installation");
                            assert!(source.has_room());
                        },
                        recovery::Policy::new(|event| reports.push(event), recover),
                    );
                    result.as_ref().map_err(Kind::from).expect("attempt source");
                });
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    assert_eq!(
        audit.faults.values().expect("exact attempt failure").len(),
        1
    );
    assert_eq!(
        (audit.sent.get(), audit.cancelled.get()),
        (Some(0), Some(0))
    );
    assert_eq!(
        [
            counts.received.get(),
            counts.connected.get(),
            counts.closed.get(),
            counts.drained.get()
        ],
        [0, 1, 1, 1]
    );
    let stream = server
        .accept_for(GUARD)
        .expect("accept replacement peer")
        .expect("connected peer");
    stream.read_wait(Some(GUARD)).expect("peer read deadline");
    assert_eq!(stream.read(&mut [0]).expect("replacement closes"), 0);
}

#[test]
fn deferred_open_retries() {
    let mut finish = Completion::new();
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind peer");
    let addr = server.local_addr().expect("peer address");
    let gate = Gate::new();
    let audit = Rc::new(Report::default());

    let planned = Attempt::new(addr)
        .prepare::<KeyTag<1>, _, Bundle<Tcp, DeferredWire, Throughput>>(Deferred {
            audit: Rc::clone(&audit),
            gate: gate.clone(),
        });
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<1>()]);

    planned
        .with_run(
            &mut finish,
            DeferredWire::default(),
            |case| reached(case.try_wait(&gate, 1), &audit),
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            leased,
        )
        .expect("deferred attempt scenario failed");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    assert_eq!(gate.hits(), 1);
    audit.checked((0, 0));
}

#[test]
fn rejected_credit_closes() {
    let mut finish = Completion::new();

    peer::TcpScript::with(
        |stream| {
            assert!(stream.write_all(b"park").is_ok(), "write parked payload");
            let mut drained = Vec::new();
            assert!(
                stream.read_to_end(&mut drained).is_ok(),
                "wait for parked connection close"
            );
        },
        |server| {
            let addr = server.addr();
            let closed = Gate::new();
            let audit = Rc::new(Report::default());
            let dropped = Rc::new(Cell::new(0));

            let planned = Attempt::new(addr)
                .prepare::<KeyTag<2>, _, Bundle<Tcp, parked::Wire, Throughput>>(Parked {
                    audit: Rc::clone(&audit),
                    closed: closed.clone(),
                    live: Cell::new(false),
                });
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<2>()]);

            planned
                .with_run(
                    &mut finish,
                    (Rc::clone(&audit), Rc::clone(&dropped)),
                    |case| reached(case.try_wait(&closed, 1), &audit),
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    leased,
                )
                .expect("parked attempt scenario failed");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            let finish = server.finish();
            assert!(finish.is_ok(), "finish receive peer");
            assert_eq!(closed.hits(), 1);
            assert_eq!(dropped.get(), 1);
            audit.checked((0, 0));
        },
    )
    .expect("spawn receive peer");
}

#[test]
fn owned_plan_reused() {
    let mut finish = Completion::new();
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind peer");
    let address = server.local_addr().expect("peer address");
    let identity = String::from("borrowed-service");
    let resolves = Rc::new(Cell::new(0));
    let ready_g = LocalGate::new();
    let ready = ready_g.handle();
    let peers = Trace::<socket::Addr, 4>::new();
    let wire = DeferredWire::default();
    let audit = Rc::new(Report::default());

    let set = Snapshot::from_one(
        Revision::new(1),
        Endpoint::new(
            identity.as_str(),
            Addr {
                socket: address,
                resolves: resolves.clone(),
            },
        ),
    );
    let node = App {
        audit: Rc::clone(&audit),
        ready,
        peers: &peers,
        wire: wire.clone(),
    };
    let install = Install::<
        KeyTag<0>,
        1,
        Bundle<Net, DeferredWire, Throughput>,
        _,
        plan::Standard,
        sync::Preserve,
        &str,
    >::new(
        set,
        1,
        plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
            .expect("valid retry bounds"),
        node,
    );
    let install = (install).expect("valid connector plan");
    let baseline = settings::Config::for_profile::<Throughput>();
    let prepared = topology::Plan::new((), install).with_install(baseline);
    let prepared = (prepared).expect("prepare service connector topology");
    let mut reclaim1 = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);

    prepared.with_local(
        |worker| {
            let result = worker.lend(|install, stage| {
                lending::Installer::<_, _>::install(install, stage, Wait(ready, &mut finish))
            });
            let result = (result).expect("install prepared service connector topology");
            assert_eq!(
                result
                    .expect("service connector application teardown")
                    .expect("service connector progress"),
                fibers::Wait::Reached
            );
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
        |entered| assert!(entered.is_ok(), "enter prepared service connector topology"),
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    audit.checked((0, 0));
    assert_eq!(ready.hits(), 1);
    assert_eq!(wire.opens(), 2);
    assert_eq!(resolves.get(), 1);
    assert!(
        peers
            .values()
            .expect("recorded observations")
            .iter()
            .copied()
            .map(|peer| peer.into_std().expect("TCP peer address"))
            .eq([address])
    );
}

fn leased<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>, lease: Option<bool>) -> T {
    let output = result.expect("native lifecycle");
    assert_eq!(lease, Some(true), "terminal attempt lease: {output:?}");
    output
}

fn reached(wait: Result<fibers::Wait, fibers::Run>, audit: &Report<error::Error, 16>) {
    audit.checked((0, 0));
    assert!(
        matches!(wait, Ok(fibers::Wait::Reached)),
        "progress: {wait:?}"
    );
}
