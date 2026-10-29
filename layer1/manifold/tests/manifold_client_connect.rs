pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    io::{Read as _, Write as _},
    net::SocketAddr,
    rc::Rc,
    time::Duration,
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
        settings,
    },
    io::socket,
};
use dope_manifold::{
    app::env::Bundle,
    network::client::topology::{Install, plan},
    service::{Endpoint, Revision, Snapshot, sync},
    timing::Throughput,
};
use dope_net::{
    link::{event::ConnectFault, pool::change::opening},
    tcp::Tcp,
    wire::{Identity, open},
};
use dope_runtime::{
    executor::session::Completion,
    topology::{self, error::Kind, lending},
};
use dope_test::{
    fibers,
    harness::{
        observe::Trace,
        peer::{self, TcpScript},
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use o3::mem::rc;

use crate::common::{
    contract::Report,
    manifold::client::{
        connect::{
            dial::empty::{self, Empty},
            open::{
                Cause, Counts, DeferredWire, Failed, Wire,
                deferred::Deferred,
                parked::{self, Parked},
            },
            rejected::Rejected,
            resolve::{Addr, App, Net, Wait},
        },
        error,
    },
    scenario::case::attempt::Attempt,
};

#[test]
fn reject_skips_callbacks() {
    let mut finish = Completion::new();

    TcpScript::with_many::<1, _>(
        |_| {},
        |server| {
            let addr = server.addr();
            let gate = Gate::new();
            let scoped = Rc::new(Cell::new(0));
            let audit = Rc::new(Report::default());

            let planned = Attempt::new(addr)
                .prepare::<KeyTag<3>, _, Bundle<Tcp, Identity, Throughput>>(Rejected {
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

            let finish = server.finish();
            assert!(finish.is_ok(), "finish held peer");
            assert_eq!(scoped.get(), 0);
            audit.checked((0, 0));
        },
    )
    .expect("spawn peer");
}

#[test]
fn reports_state_failure() {
    let mut finish = Completion::new();

    use std::io::Read as _;

    TcpScript::with(
        |stream| {
            let mut bytes = Vec::new();
            assert!(stream.read_to_end(&mut bytes).is_ok());
            assert!(bytes.is_empty());
        },
        |server| {
            let done = Gate::new();
            let trace = Rc::new(empty::Trace::default());
            let audit = Rc::new(Report::default());

            let planned = Attempt::new(server.addr())
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
            assert!(server.finish().is_ok());
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
        },
    )
    .expect("the state rejection peer must bind");
}

#[test]
fn fatal_open_reports_once() {
    let mut finish = Completion::new();
    let addr = SocketAddr::from(([127, 0, 0, 1], 9));
    let gate = Gate::new();
    let failures = Rc::new(Cell::new(0));
    let audit = Rc::new(Report::default());
    let counts = Rc::new(Counts::default());

    let planned =
        Attempt::new(addr).prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>(Failed {
            check: |cause: opening::Failure<open::Error<'_, Wire>>| {
                let error = cause
                    .into_wire()
                    .expect("wire failure test must not exhaust connection identities");
                assert_eq!(error.cause(), &Cause::Permanent);
            },
            audit: Rc::clone(&audit),
            counts: Rc::clone(&counts),

            gate: gate.clone(),
            failures: Rc::clone(&failures),
        });
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            (),
            |case| reached(case.try_wait(&gate, 1), &audit),
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            leased,
        )
        .expect("fatal-open attempt scenario failed");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    assert_eq!(failures.get(), 1);
    assert_eq!(
        [
            counts.received.get(),
            counts.connected.get(),
            counts.prepared.get(),
            counts.closed.get(),
            counts.drained.get()
        ],
        [0; 5]
    );
    audit.checked((0, 0));
}

#[test]
fn deferred_open_retries() {
    let mut finish = Completion::new();

    TcpScript::with_many::<1, _>(
        |_| {},
        |server| {
            let addr = server.addr();
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

            let finish = server.finish();
            assert!(finish.is_ok(), "finish peer");
            assert_eq!(gate.hits(), 1);
            audit.checked((0, 0));
        },
    )
    .expect("spawn peer");
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

    TcpScript::with_many::<1, _>(
        |_| {},
        |server| {
            let address = server.addr();
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
                        lending::Installer::<_, _>::install(
                            install,
                            stage,
                            Wait(ready, &mut finish),
                        )
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
            assert!(server.finish().is_ok(), "finish held peer");
        },
    )
    .expect("spawn held peer");
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
