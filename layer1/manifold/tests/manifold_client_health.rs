pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io,
    net::TcpListener,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    platform::wake,
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
use dope_runtime::executor::{
    self, Executor,
    session::{Completion, Scope},
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer::{AcceptScript, TcpScript},
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
    scenario::case::{Connector, Value},
};

#[test]
fn stale_epoch_is_ignored() {
    let mut finish = Completion::new();
    const BACKOFF: Duration = Duration::from_secs(3);

    let (a_rx, a_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    let (b_rx, b_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    let (end_rx, end_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    AcceptScript::with(
        move |acceptor| {
            let first = acceptor.accept()?;
            let second = acceptor.accept()?;
            assert!(a_rx.wait_timeout(GUARD).is_ok(), "release first connection");
            drop(first);
            let probe = acceptor.accept()?;
            assert!(b_rx.wait_timeout(GUARD).is_ok(), "release stale connection");
            drop(second);
            let next = acceptor.accept()?;
            assert!(
                end_rx.wait_timeout(GUARD).is_ok(),
                "release recovered connections"
            );
            drop((probe, next));
            Ok::<_, io::Error>(())
        },
        |server| {
            let addr = server.addr();
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

            let latency = planned.with_run(
                &mut finish,
                |case| {
                    assert_eq!(
                        case.try_wait(&conn, 2).expect("progress wait"),
                        fibers::Wait::Reached
                    );
                    assert!(a_tx.notify().is_ok(), "drop first connection");
                    assert_eq!(
                        case.try_wait(&closed, 1).expect("progress wait"),
                        fibers::Wait::Reached
                    );
                    assert_eq!(
                        case.try_wait(&conn, 3).expect("progress wait"),
                        fibers::Wait::Reached
                    );
                    let start = Instant::now();
                    assert!(b_tx.notify().is_ok(), "drop stale connection");
                    assert_eq!(
                        case.try_wait(&closed, 2).expect("progress wait"),
                        fibers::Wait::Reached
                    );
                    assert_eq!(
                        case.try_wait(&conn, 4).expect("progress wait"),
                        fibers::Wait::Reached
                    );
                    start.elapsed()
                },
                recovery::Policy::new(|event| reclaim1.push(event), recover),
                |result| result.expect("native lifecycle"),
            );
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            let latency = latency.expect("health epoch scenario");
            audit.checked((0, 0));
            assert_eq!(conn.hits(), closed.hits());
            assert!(end_tx.notify().is_ok());
            let finished = (server.finish()).expect("finish health endpoint");
            assert!(finished.is_ok(), "health endpoint I/O");
            assert!(latency < BACKOFF / 2);
        },
    )
    .expect("spawn health endpoint");
}

#[test]
fn next_endpoint_connects() {
    let mut finish = Completion::new();
    let bad = {
        let refused = TcpListener::bind("127.0.0.1:0").expect("reserve refused address");
        refused.local_addr().expect("read refused address")
    };
    TcpScript::with_many::<1, _>(|_| {}, |server| {
        let good = server.addr();
        let gate = Gate::new();
        let plan = (plan::Plan::<KeyTag<0>, 1>::new(
            1,
            plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
                .expect("valid retry bounds"),
        ))
        .expect("connector capacity");

        let config = (plan.configured()).expect("configure executor");
        use dope_core::driver::life::creation::Attempt;
        let mut create = Attempt::new();
        let result = Executor::new(&mut create, config);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("create executor");
        let Ok(executor) = result else {
            return;
        };
        let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

        executor.with_storage(()).run(|executor| {
            let result = executor.enter_with(
                |mut runtime| {
                    use executor::session::Scope;

                    let events = Rc::new(Cell::new(Events::default()));
                    let errors = Rc::new(Trace::new());
                    let backoff = runtime.hash_state(Domain::DEFAULT);
                    let set = Snapshot::from_vec(
                        Revision::new(1),
                        array::Vec::from_fn(Len::exact::<2>(), |slot| {
                            let addr = if slot.index() == 0 { bad } else { good };
                            Endpoint::new(addr, addr)
                        }),
                    );
                    let audit = Rc::new(Report::default());
                    let target = Session {
                        audit: audit.clone(),

                        codec: codec::Outbound,
                        errors: errors.clone(),
                        events: events.clone(),
                        gate: gate.clone(),
                    };
                    let setup = session::Setup::new(target, ingress::Plan::none());
                    let result =
                        plan.bind(domain::Local::new(Identity), backoff)
                            .install::<_, _, sync::Preserve, _, Bundle<Tcp, Identity, Balanced>, 16, _, _>(
                        setup,
                        set,
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let result = runtime.with_quiesce(&mut finish,
                                Host::new(host),
                                Duration::from_secs(2),
                                |mut app| {
                                    assert_eq!(fibers::TEST.try_wait(&mut app, &gate, 1).expect("driver wait"), fibers::Wait::Reached);
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
                    assert!(observed.is_ordered(), "{:?}", (&observed.conn, &observed.closed, &observed.quarantined));
                    assert!(
                        observed.peers
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

            assert!(result.is_ok(), "executor teardown");

            assert!(server.finish().is_ok(), "finish held peer");
        });
    }).expect("spawn held peer");
}

/// Successful connections and neutral lifetime rotations preserve endpoint selection order.
#[test]
fn recovery_keeps_turn() {
    let mut finish = Completion::new();
    let (first_rx, first_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    let (last_rx, last_tx) = wake::Ends::blocking()
        .expect("peer release channel")
        .split();
    TcpScript::with_many::<2, _>(
        move |_| {
            first_rx
                .wait_timeout(GUARD)
                .expect("release first endpoint");
        },
        |first| {
            TcpScript::with_many::<2, _>(
                move |_| {
                    last_rx
                        .wait_timeout(GUARD)
                        .expect("release second endpoint");
                },
                |second| {
                    let addrs = [first.addr(), second.addr()];
                    let chosen = Rc::new(Trace::new());
                    let opened = Gate::new();
                    let plan = Plan::<KeyTag<0>, 2>::new(
                        2,
                        Spec::new(Duration::from_secs(2), Duration::from_secs(2))
                            .expect("valid retry bounds"),
                    )
                    .expect("two connections")
                    .with_ttl(Positive::from_millis::<100>());
                    let config = plan.configured().expect("round robin resources");
                    use dope_core::driver::life::creation::Attempt;
                    let mut create = Attempt::new();
                    let result = Executor::new(&mut create, config);
                    result
                        .as_ref()
                        .map_err(|error| error.kind())
                        .expect("executor construction");
                    let executor = result.ok().expect("executor construction");
                    let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

                    executor.with_storage(()).run(|executor| {
                        let result = executor.enter_with(
                            |mut runtime| {
                                use dope_manifold::client::session::ingress::Plan;

                                let events = Rc::new(Cell::new(Events::default()));
                                let errors = Rc::new(Trace::new());
                                let audit = Rc::new(Report::default());
                                let target = Session {
                                    audit: audit.clone(),
                                    codec: codec::Outbound,
                                    errors: errors.clone(),
                                    events: events.clone(),
                                    gate: opened.clone(),
                                };
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

                        assert!(result.is_ok(), "round robin executor teardown");
                        first_tx.notify().expect("finish first endpoint");
                        last_tx.notify().expect("finish second endpoint");
                        assert!(first.finish().is_ok());
                        assert!(second.finish().is_ok());
                    });
                },
            )
            .expect("second endpoint");
        },
    )
    .expect("first endpoint");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
