pub mod common;

use std::{
    cell::Cell,
    fmt::Debug,
    net,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
        schedule::ready,
    },
    io::socket,
};
use dope_manifold::{
    app::env::Bundle,
    client::{close::Reason, deadline},
    timing::{Balanced, Throughput},
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    fibers::Wait,
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        finish::{
            lifetime::{self, LIMIT, Lifetime, Profile},
            shutdown::{CONNECTIONS, Shutdown},
        },
        probe::{self, IDLE_TIMEOUT, Probe, REDIAL_BACKOFF},
    },
    recover,
    scenario::case::{Connector, Value},
};

#[test]
fn age_timeout_is_typed() {
    let mut finish = Completion::new();
    let server = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("bind peers");
    let addr = server.local_addr().expect("peer address");
    let ready_g = LocalGate::new();
    let ready = ready_g.handle();
    let events = Trace::<lifetime::Event, 16>::new();
    let audit = Rc::new(Report::default());

    let planned = Connector::<{ lifetime::MAX }>::new(
        addr,
        Duration::from_millis(20),
        Duration::from_millis(200),
    )
    .lifetime(LIMIT)
    .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Profile>>(Value::new(Lifetime {
        audit: audit.clone(),

        ready,
        events: &events,
    }))
    .expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                assert_eq!(
                    case.try_wait(&ready, 2).expect("progress wait"),
                    fibers::Wait::Reached
                )
            },
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            native,
        )
        .expect("application scenario");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    audit.checked((0, 0));
    assert_eq!(ready.hits(), 2);
    let events = events.values().expect("lifetime lifecycle observations");
    assert!(
        matches!(events.as_ref(), [
                    lifetime::Event::Connected(Some(first)),
                    lifetime::Event::Quarantined(1),
                    lifetime::Event::Closed(Reason::Timeout(deadline::Kind::Lifetime)),
                    lifetime::Event::Connected(Some(second)),
                    lifetime::Event::Quarantined(2),
                    lifetime::Event::Closed(_),
                ] if first.is_none_or(|left| left <= LIMIT.get()) && second.is_none_or(|left| left <= LIMIT.get())),
        "{events:?}"
    );
}

#[test]
fn drains_over_turn_limit() {
    let mut finish = Completion::new();
    let listener = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let mut held = [const { None }; CONNECTIONS];
    let ready = Gate::new();
    let closed = Gate::new();
    let audit = Rc::new(Report::default());

    let planned =
        Connector::<CONNECTIONS>::new(address, Duration::from_millis(1), Duration::from_millis(1))
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>(Value::new(Shutdown {
                audit: audit.clone(),

                conn: ready.clone(),
                closed: closed.clone(),
            }))
            .expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                for (index, slot) in held.iter_mut().enumerate() {
                    assert_eq!(
                        case.try_wait(&ready, index as u32 + 1).expect("connect"),
                        fibers::Wait::Reached
                    );
                    *slot = Some(
                        listener
                            .accept_for(GUARD)
                            .expect("accept")
                            .expect("connected peer"),
                    );
                }
            },
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            native,
        )
        .expect("application scenario");
    assert!(
        matches!(reclaim1.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reclaim1:?}"
    );

    assert_eq!(closed.hits(), CONNECTIONS as u32);
    assert_eq!(ready.hits(), CONNECTIONS as u32);
    audit.checked((0, 0));
    drop(held);
}

#[test]
fn shutdown_does_not_poll_ready_discovery() {
    use dope_core::driver::{life::creation::Attempt, schedule};
    use dope_manifold::{
        app::Host,
        client::session::{Setup, ingress},
        network::client::topology::plan::{Plan, Spec},
        service::{Endpoint, Revision, Snapshot, health::Domain, sync::Replace},
    };
    use dope_net::wire::domain::Local;
    use dope_runtime::executor::{Executor, session::Scope};

    use crate::common::{
        contract::codec,
        manifold::client::{
            Configure as _,
            finish::close::{
                members::{Source, Watch},
                observe::Session,
            },
            side::limited::Limited,
        },
    };

    let plan = Plan::<KeyTag<0>, { schedule::LIMIT + 2 }>::new(
        schedule::LIMIT + 2,
        Spec::new(Duration::from_secs(1), Duration::from_secs(1)).expect("retry bounds"),
    )
    .expect("empty connection capacity");
    let config = plan.configured().expect("client resources");
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, config).ok().expect("executor");
    let mut reports = Reports::<_>::new(config, [Id::fixed::<0>()]);
    let mut finish = Completion::new();
    let pending = Rc::new(Cell::new(None));
    executor.run(|executor| {
        let result = executor.enter_with(
            |mut runtime| {
                let setup = Setup::new(Session::new(codec::Outbound), ingress::Plan::none());
                let hash = runtime.hash_state(Domain::DEFAULT);
                plan.bind(Local::new(Identity), hash)
                    .discover::<_, _, _, Replace, _, _, Bundle<Tcp, Identity, Balanced>, 1, _, _>(
                        setup,
                        Source {
                            pending: pending.clone(),
                            failures: Gate::new(),
                        },
                        Watch(Rc::new(Trace::new())),
                        &mut runtime,
                        Scope::driver,
                        |pool, runtime| {
                            let now = runtime.driver().driver_ref().clock().now();
                            let snapshot = Snapshot::<Endpoint<u32, net::SocketAddr>, 1>::empty(
                                Revision::new(1),
                            );
                            runtime
                                .with_at_for(
                                    &mut finish,
                                    Host::new(Limited { inner: pool }),
                                    now.into(),
                                    Duration::from_secs(2),
                                    |_| pending.set(Some(snapshot)),
                                )
                                .expect("shutdown completes with ready discovery");
                        },
                    )
                    .expect("install discovery connector");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "executor teardown");
    });
    assert!(pending.take().is_some(), "shutdown preserves source work");
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
}

#[test]
fn idle_timeout_controls_redial() {
    for timeout in [Some(IDLE_TIMEOUT), None] {
        let mut finish = Completion::new();
        let listener =
            peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let gate_g = LocalGate::new();
        let gate = gate_g.handle();
        let redial = Cell::new(None);
        let audit = Rc::new(Report::default());

        let planned = Connector::<{ probe::MAX }>::new(addr, REDIAL_BACKOFF, REDIAL_BACKOFF)
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Value::new(Probe {
                audit: audit.clone(),

                timeout,
                peer: move |peer: socket::Addr, _: ready::Target<'_>| {
                    assert_eq!(peer.into_std().expect("TCP peer address"), addr)
                },
                gate,
                start: Instant::now(),
                redial: &redial,
                rejected: None,
                expired: None,
            }))
            .expect("prepare actual client layout");
        let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

        let reached = planned
            .with_run(
                &mut finish,
                |case| case.try_wait(&gate, 2),
                recovery::Policy::new(|event| reclaim1.push(event), recover),
                native,
            )
            .expect("liveness scenario")
            .expect("wait for liveness scenario");
        assert!(
            matches!(reclaim1.events(), Ok(events) if events.is_empty()),
            "unexpected reclaimed operations: {reclaim1:?}"
        );

        audit.checked((0, 0));
        if let Some(timeout) = timeout {
            assert_eq!(reached, Wait::Reached);
            assert_eq!(gate.hits(), 2);
            let dt = redial.get().expect("must reconnect after silence");
            assert!(dt >= timeout.get() - Duration::from_millis(30));
            assert!(dt < timeout.get() * 8, "reconnect at {dt:?}");
        } else {
            assert_eq!(reached, Wait::Deadline);
            assert_eq!(gate.hits(), 1);
            assert_eq!(redial.get(), None);
        }
    }
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}
