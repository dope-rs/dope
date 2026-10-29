pub mod common;

use std::{convert::Infallible, io::Read as _, net::SocketAddr, rc::Rc, time::Duration};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
        schedule::ready,
        settings,
    },
    platform::wake,
};
use dope_manifold::{
    app::{self, env},
    client::{
        session::{self, ingress},
        side::{Enabled, Error, registry},
    },
    network::client::topology::plan,
    service::{self, health, sync},
    timing::Balanced,
};
use dope_net::{
    tcp,
    wire::{self, domain},
};
use dope_runtime::{
    executor::session::Completion,
    topology::{Plan, lending},
};
use dope_test::{
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{LocalGate, LocalGateRef, Progress as _},
    },
    reclaim::Reports,
};
use o3::{buffer::storage::Shared, depot::map::direct};

use crate::common::{
    contract::Report,
    manifold::client::side::{
        Reservation, Storage,
        app::{App, Wait},
    },
};

#[test]
fn fresh_uses_peer() {
    let (end_rx, end_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    peer::TcpScript::with_many::<2, _>(
        move |streams| {
            let [main, aux] = streams;
            let primary = (main.peer_addr()).expect("main peer address");
            let aux_addr = (aux.peer_addr()).expect("side peer address");
            assert!(primary != aux_addr);
            let mut payload = [0; 6];
            assert!(aux.read_exact(&mut payload).is_ok(), "side payload");
            assert_eq!(payload, *b"cancel");
            assert!(end_rx.wait_timeout(GUARD).is_ok(), "release server");
        },
        |server| {
            let addr = server.addr();

            let joining = LocalGate::new();
            let conn = joining.handle();
            let sending = LocalGate::new();
            let done = sending.handle();
            let stopping = LocalGate::new();
            let closed = stopping.handle();
            let events = Trace::new();
            run(addr, false, conn, done, closed, &events);

            assert!(*events.values().expect("recorded observations") == [Ok(())]);
            assert!(end_tx.notify().is_ok(), "release server");
            assert!(server.finish().is_ok(), "finish server");
        },
    )
    .expect("spawn server");
}

#[test]
fn abandon_drains_lane() {
    let (end_rx, end_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    peer::TcpScript::with(
        move |_| {
            assert!(end_rx.wait_timeout(GUARD).is_ok(), "release server");
        },
        |server| {
            let addr = server.addr();

            let joining = LocalGate::new();
            let conn = joining.handle();
            let sending = LocalGate::new();
            let done = sending.handle();
            let stopping = LocalGate::new();
            let closed = stopping.handle();
            let events = Trace::new();
            run(addr, true, conn, done, closed, &events);

            assert!(events.values().expect("recorded observations").is_empty());
            assert!(end_tx.notify().is_ok(), "release server");
            assert!(server.finish().is_ok(), "finish server");
        },
    )
    .expect("spawn server");
}

#[test]
fn startup_hides_target() {
    let joining = LocalGate::new();
    let sending = LocalGate::new();
    let closing = LocalGate::new();
    let events = Trace::new();
    let plan = plan::Plan::<KeyTag<0>, 1, plan::Side>::try_new(
        1,
        plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
            .expect("valid retry bounds"),
    )
    .expect("valid side connector plan");
    let audit = Rc::new(Report::default());
    let app = App {
        audit: Rc::clone(&audit),
        create: || Shared::try_copy(b"cancel").expect("allocate side payload"),
        ready: side_ready,
        conn: joining.handle(),
        done: sending.handle(),
        closed: closing.handle(),
        events: &events,
        eager: false,
    };
    let prepared = Plan::new(Storage, (plan, app))
        .with_install(settings::Config::for_profile::<Balanced>())
        .expect("prepare side connector topology");
    let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
    prepared.try_scoped(
        |scope| {
            scope.lend(|(plan, app), stage| {
                let claims = Rc::new(Trace::new());
                let timer = stage.driver().driver_ref().timer();
                let mut alarms = Vec::new();
                while let Ok(alarm) = timer.try_alarm() {
                    alarms.push(alarm);
                }
                assert!(alarms.pop().is_some());
                assert!(alarms.pop().is_some());
                let setup = session::Setup::new(app.build(stage.storage(), Rc::clone(&claims)), ingress::Plan::none());
                let mode = Enabled::new(stage.storage());
                let hash = stage.hash_state(health::Domain::for_id(0));
                let plan = plan.bind(domain::Local::new(wire::Identity), hash);
                let addr = SocketAddr::from(([127, 0, 0, 1], 1));
                let snapshot = service::Snapshot::from_one(
                    service::Revision::new(1),
                    service::Endpoint::new(addr, addr),
                );
                let result = plan.install::<_, _, _, sync::Preserve, _, env::Bundle<tcp::Tcp, wire::Identity, Balanced>, 16, _, _>(
                    setup, mode, snapshot, stage, lending::Stage::driver,
                    |_, _| (),
                );
                assert!(matches!(result, Err(app::creation::Error::Timer(_))));
                assert!(stage.storage().ready_target().is_none());
                assert!(claims.values().expect("startup claims").is_empty());
                drop(alarms);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |entered| assert!(entered.is_ok()),
    );
    audit.checked((0, 0));
    assert!(events.values().expect("startup observations").is_empty());
    assert!(
        matches!(reports.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn run(
    addr: SocketAddr,
    eager: bool,
    conn: LocalGateRef<'_>,
    done: LocalGateRef<'_>,
    closed: LocalGateRef<'_>,
    events: &Trace<Result<(), Error>, 4>,
) {
    let mut finish = Completion::new();
    let snapshot = service::Snapshot::from_one(
        service::Revision::new(1),
        service::Endpoint::new(addr, addr),
    );
    let plan = plan::Plan::<KeyTag<0>, 1, plan::Side>::try_new(
        1,
        plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
            .expect("valid retry bounds"),
    )
    .expect("valid side connector plan");
    let audit = Rc::new(Report::default());
    let app = App {
        audit: Rc::clone(&audit),
        create: || Shared::try_copy(b"cancel").expect("allocate side payload"),
        ready: side_ready,
        conn,
        done,
        closed,
        events,
        eager,
    };
    let baseline = settings::Config::for_profile::<Balanced>();
    let prepared = Plan::new(Storage, (plan, app)).with_install(baseline);
    let prepared = (prepared).expect("prepare side connector topology");
    let mut reclaim1 = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);

    prepared.try_scoped(
        |scope| {
            let result = scope.lend(|(plan, app), stage| {
                let claims = Rc::new(Trace::new());
                let source = app.build(stage.storage(), Rc::clone(&claims));
                let setup = session::Setup::new(source, ingress::Plan::none());
                let mode = Enabled::new(stage.storage());
                let wire = domain::Local::new(wire::Identity);
                let hash = stage.hash_state(health::Domain::for_id(0));
                let plan = plan.bind(wire, hash);
                let result = plan.install::<_, _, _, sync::Preserve, _, env::Bundle<tcp::Tcp, wire::Identity, Balanced>, 16, _, _>(
                    setup, mode, snapshot, stage, lending::Stage::driver,
                    |pool, stage| lending::Next::<_, _>::run(
                        Wait { conn, done, finish: &mut finish },
                        app::Host::new(pool), stage,
                    ),
                );
                let observed = claims.values().expect("reservation observations");
                assert!(matches!(&*observed, [
                    Reservation { target: first, result: Ok(()), .. },
                    Reservation { target, address, result: Err((registry::Cause::Slot(direct::Error::Occupied { key, stored }), returned, actual)) }
                ] if first == target && key == stored && key.into_key().index() == target.index()
                    && returned == target && actual == address), "{observed:?}");
                result
            });
            audit.checked((0, 0));
            let result = (result).expect("install prepared side connector topology");
            assert_eq!(
                result
                    .expect("side connector application teardown")
                    .expect("side connector progress"),
                (dope_test::fibers::Wait::Reached, Some(dope_test::fibers::Wait::Reached))
            );
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
        |entered| {
            assert!(
                entered.is_ok(),
                "enter prepared side connector topology: {:?}",
                entered.as_ref().err().map(|error| error.kind())
            );
        },
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    audit.checked((0, 0));
    assert_eq!((conn.hits(), done.hits(), closed.hits()), (1, 1, 1));
}

fn side_ready<'d>(target: Option<ready::Target<'d>>) -> ready::Target<'d> {
    target.expect("side registry is running")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
