pub mod common;

use std::{
    cell::Cell,
    net::TcpListener,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::session::{self, Setup, ingress},
    network::client::topology::plan::{self, Plan, Spec},
    service::{
        Endpoint, Revision, Snapshot,
        health::Domain,
        sync::{self, Preserve},
    },
    timing::Balanced,
};
use dope_net::{
    tcp::Tcp,
    wire::{
        Identity,
        domain::{self, Local},
    },
};
use dope_runtime::executor::session::{Completion, Scope};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer::TcpAcceptor,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{
    base::time::Positive,
    depot::array::{self, Len},
};

use crate::common::{
    contract::{Report, codec},
    manifold::client::{
        Configure as _,
        finish::close::{
            Epoch,
            members::{Address, Net, Until},
            observe::{Events, Session},
        },
    },
    recover,
    scenario::{
        Runtime,
        case::{Connector, Value},
    },
};

#[test]
fn due_backoff_survives_exhausted_maintenance() {
    use dope_core::driver::settings;
    use dope_runtime::topology::{self, lending};

    use crate::common::manifold::{
        client::backoff::{Exhausted, Injection},
        root::Root,
    };

    let mut finish = Completion::new();
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind retry endpoint");
    let injected = Cell::new(Injection::Waiting);
    let polls = Cell::new(0);
    let chosen = Rc::new(Trace::new());
    let audit = Rc::new(Report::default());
    let backoff = Duration::from_millis(100);
    let plan = Plan::<KeyTag<0>, 2>::new(2, Spec::new(backoff, backoff).expect("retry bounds"))
        .expect("two connections");
    let prepared = topology::Plan::new((), plan)
        .with_install(settings::Config::for_profile::<Balanced>())
        .expect("client layout");
    let mut reports = Reports::<_>::new(prepared.layout(), [Id::fixed::<0>()]);
    prepared.with_local(
        |scope| {
            let result = scope.lend(|plan, stage| {
                let mut target = Session::new(codec::Pending::<1>);
                target.audit = audit.clone();
                let events = target.events.clone();
                let conn = target.gate.clone();
                let setup = Setup::new(target, ingress::Plan::none());
                let addr = server.local_addr().expect("retry endpoint address");
                let snapshot = Snapshot::from_one(
                    Revision::new(1),
                    Endpoint::new(
                        addr,
                        Address {
                            addr,
                            chosen: chosen.clone(),
                        },
                    ),
                );
                let hash = stage.hash_state(Domain::DEFAULT);
                plan.bind(Local::new(Identity), hash)
                    .install::<_, _, Preserve, _, Bundle<Net, Identity, Balanced>, 1, _, _>(
                        setup,
                        snapshot,
                        stage,
                        lending::Stage::driver,
                        |pool, stage| {
                            let now = stage.driver().driver_ref().clock().now();
                            let due = now.clamp_add(backoff);
                            let host = Exhausted::new(pool, due, &injected, |mut pool| {
                                let observed = Events::snapshot(&events);
                                let retired = observed.closed[0].0;
                                let live = observed
                                    .conn
                                    .into_iter()
                                    .find(|conn| *conn != retired)
                                    .expect("retained connection");
                                assert!(matches!(
                                    pool.as_mut().wake(live),
                                    dope_manifold::client::app::Wake::Ready
                                ));
                                live.lane().slot()
                            });
                            stage.with_at_for(
                                &mut finish,
                                Host::new(host),
                                now.into(),
                                Duration::from_secs(2),
                                |mut app| {
                                    app.drive_for(now.into(), GUARD, Until(|| conn.hits() == 2))
                                        .expect("initial connections")
                                        .into_output();
                                    let [first, retained] = [(); 2].map(|()| {
                                        server
                                            .accept_for(GUARD)
                                            .expect("accept initial peer")
                                            .expect("initial peer")
                                    });
                                    drop(first);
                                    app.drive_for(
                                        now.into(),
                                        GUARD,
                                        Until(|| Events::snapshot(&events).closed.len() == 1),
                                    )
                                    .expect("one retired epoch")
                                    .into_output();
                                    let initial = chosen.values().expect("initial attempts").len();
                                    app.drive_for(due, GUARD, Root(&polls))
                                        .expect("fixed point after due backoff")
                                        .into_output();
                                    assert_eq!(initial, 2);
                                    assert_eq!(
                                        chosen.values().expect("prepared attempts").len(),
                                        3
                                    );
                                    retained
                                },
                            )
                        },
                    )
            });
            let _retained = result.expect("install client").expect("client teardown");
            assert_eq!(injected.get(), Injection::Done);
            assert_eq!(polls.get(), 1, "completed root never rescues progress");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |entered| assert!(entered.is_ok()),
    );
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    audit.checked((0, 0));
}

#[test]
fn stale_epoch_is_ignored() {
    let mut finish = Completion::new();
    const BACKOFF: Duration = Duration::from_secs(3);

    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind health endpoint");
    let addr = server.local_addr().expect("health endpoint address");
    let conn = Gate::new();
    let closed = Gate::new();
    let audit = Rc::new(Report::default());

    let planned = Connector::<2>::new(addr, BACKOFF, BACKOFF)
        .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>(Value::new(Epoch {
            audit: audit.clone(),

            conn: conn.clone(),
            closed: closed.clone(),
        }))
        .expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    let peers = planned.with_run(
        &mut finish,
        |case| {
            assert_eq!(
                case.try_wait(&conn, 2).expect("progress wait"),
                fibers::Wait::Reached
            );
            let [first, second] = [(); 2].map(|()| {
                server
                    .accept_for(GUARD)
                    .expect("accept initial peer")
                    .expect("initial peer")
            });
            drop(first);
            assert_eq!(
                case.try_wait(&closed, 1).expect("progress wait"),
                fibers::Wait::Reached
            );
            assert_eq!(
                case.try_wait(&conn, 3).expect("progress wait"),
                fibers::Wait::Reached
            );
            let probe = server
                .accept_for(GUARD)
                .expect("accept probe peer")
                .expect("probe peer");
            let start = Instant::now();
            drop(second);
            assert_eq!(
                case.try_wait(&closed, 2).expect("progress wait"),
                fibers::Wait::Reached
            );
            assert_eq!(
                case.try_wait(&conn, 4).expect("progress wait"),
                fibers::Wait::Reached
            );
            let latency = start.elapsed();
            let next = server
                .accept_for(GUARD)
                .expect("accept next peer")
                .expect("next peer");
            assert!(latency < BACKOFF / 2);
            (probe, next)
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
        |result| result.expect("native lifecycle"),
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    let (_probe, _next) = peers.expect("health epoch scenario");
    audit.checked((0, 0));
    assert_eq!(conn.hits(), closed.hits());
}

#[test]
fn next_endpoint_connects() {
    let mut finish = Completion::new();
    let bad = {
        let refused = TcpListener::bind("127.0.0.1:0").expect("reserve refused address");
        refused.local_addr().expect("read refused address")
    };
    let server = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind good endpoint");
    let good = server.local_addr().expect("good endpoint address");
    let plan = (plan::Plan::<KeyTag<0>, 1>::new(
        1,
        plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
            .expect("valid retry bounds"),
    ))
    .expect("connector capacity");

    let config = (plan.configured()).expect("configure executor");
    let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

    Runtime::new(config).with_scope(
        |mut runtime| {
            let target = Session::new(codec::Outbound);
            let events = target.events.clone();
            let errors = target.errors.clone();
            let audit = target.audit.clone();
            let gate = target.gate.clone();
            let backoff = runtime.hash_state(Domain::DEFAULT);
            let set = Snapshot::from_vec(
                Revision::new(1),
                array::Vec::from_fn(Len::exact::<2>(), |slot| {
                    let addr = if slot.index() == 0 { bad } else { good };
                    Endpoint::new(addr, addr)
                }),
            );
            let setup = session::Setup::new(target, ingress::Plan::none());
            let result = plan
                .bind(domain::Local::new(Identity), backoff)
                .install::<_, _, sync::Preserve, _, Bundle<Tcp, Identity, Balanced>, 16, _, _>(
                    setup,
                    set,
                    &mut runtime,
                    Scope::driver,
                    |host, runtime| {
                        let result = runtime.with_quiesce(
                            &mut finish,
                            Host::new(host),
                            Duration::from_secs(2),
                            |mut app| {
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &gate, 1)
                                        .expect("driver wait"),
                                    fibers::Wait::Reached
                                );
                                drop(
                                    server
                                        .accept_for(GUARD)
                                        .expect("accept good peer")
                                        .expect("connected good peer"),
                                );
                            },
                        );
                        assert!(result.is_ok(), "application teardown");
                    },
                );
            assert!(result.is_ok(), "create connector");
            audit.checked((0, 0));
            let observed = Events::snapshot(&events);
            assert_eq!(gate.hits() as usize, observed.conn.len());
            assert!(observed.replies.is_some(), "reply count overflowed");
            assert!(
                observed.is_ordered(),
                "{:?}",
                (&observed.conn, &observed.closed, &observed.quarantined)
            );
            assert!(
                observed
                    .peers
                    .iter()
                    .copied()
                    .map(|peer| peer.into_std().expect("TCP peer address"))
                    .eq([good])
            );
            assert!(errors.values().expect("codec observations").is_empty());
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );
}

/// Successful connections and neutral lifetime rotations preserve endpoint selection order.
#[test]
fn recovery_keeps_turn() {
    let mut finish = Completion::new();
    let servers =
        [(); 2].map(|()| TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("bind endpoint"));
    let addrs = servers
        .each_ref()
        .map(|server| server.local_addr().expect("endpoint address"));
    let chosen = Rc::new(Trace::new());
    let plan = Plan::<KeyTag<0>, 2>::new(
        2,
        Spec::new(Duration::from_secs(2), Duration::from_secs(2)).expect("valid retry bounds"),
    )
    .expect("two connections")
    .with_ttl(Positive::from_millis::<100>());
    let config = plan.configured().expect("round robin resources");
    let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

    Runtime::new(config).with_scope(
        |mut runtime| {
            use dope_manifold::client::session::ingress::Plan;

            let target = Session::new(codec::Outbound);
            let events = target.events.clone();
            let errors = target.errors.clone();
            let audit = target.audit.clone();
            let opened = target.gate.clone();
            let set = Snapshot::from_vec(
                Revision::new(1),
                addrs
                    .map(|addr| {
                        Endpoint::new(
                            addr,
                            Address {
                                addr,
                                chosen: chosen.clone(),
                            },
                        )
                    })
                    .into(),
            );
            let hash = runtime.hash_state(Domain::DEFAULT);
            let result = plan
                .bind(Local::new(Identity), hash)
                .install::<_, _, Preserve, _, Bundle<Net, Identity, Balanced>, 2, _, _>(
                    Setup::new(target, Plan::none()),
                    set,
                    &mut runtime,
                    Scope::driver,
                    |host, runtime| {
                        let now = runtime.driver().driver_ref().clock().now();
                        let result = runtime.with_at_for(
                            &mut finish,
                            Host::new(host),
                            now.into(),
                            Duration::from_secs(2),
                            |mut app| {
                                app.drive(now.into(), Until(|| opened.hits() == 2))
                                    .expect("both endpoints recover")
                                    .into_output();
                                assert_eq!(*chosen.values().expect("chosen endpoints"), addrs);
                                app.drive(
                                    now.clamp_add(Duration::from_millis(100)),
                                    Until(|| opened.hits() == 4),
                                )
                                .expect("both endpoints rotate without backoff")
                                .into_output();
                                assert_eq!(Events::snapshot(&events).closed.len(), 2);
                            },
                        );
                        assert!(result.is_ok(), "round robin application teardown");
                    },
                );
            assert!(result.is_ok(), "install round robin connector");
            audit.checked((0, 0));
            assert_eq!(
                *chosen.values().expect("chosen endpoints"),
                [addrs[0], addrs[1], addrs[0], addrs[1]]
            );
            assert!(errors.values().expect("codec observations").is_empty());
            let observed = Events::snapshot(&events);
            assert_eq!(opened.hits() as usize, observed.conn.len());
            assert!(observed.replies.is_some(), "reply count overflowed");
            assert!(
                observed.is_ordered(),
                "{:?}",
                (&observed.conn, &observed.closed, &observed.quarantined)
            );
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
    );
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    for server in servers {
        for _ in 0..2 {
            server
                .accept_for(GUARD)
                .expect("accept endpoint peer")
                .expect("connected endpoint peer");
        }
    }
}
