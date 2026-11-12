pub mod common;

use crate::common::manifold::{
    client::{assert_reclaimed, leased},
    native,
};

use std::{cell::Cell, net, rc::Rc};

use dope_core::{
    driver::{
        life::recovery,
        result::SubmitError,
        route::{Id, KeyTag},
        schedule,
    },
    io::socket::option,
};
use dope_manifold::{app::env::Bundle, timing::Throughput};
use dope_net::{
    link::{egress, pool::change::opening},
    tcp::{StreamConfig, Tcp},
    wire::Identity,
};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer::{self, TcpScript},
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{buffer::storage::Shared, depot::arena::slab::Vacancy};

use crate::common::{
    contract::Report,
    manifold::client::{
        error,
        kind::{
            bounded::{Bounded, Phase},
            redial::{BACKOFF, Exhaustion, Mode, Redial},
            yielding::{Life, Yielding},
        },
    },
    recover,
    scenario::case::{Connector, Value, attempt::Attempt},
};

#[test]
fn drain_resumes_next_turn() {
    let mut finish = Completion::new();

    TcpScript::with(
        |stream| {
            let mut bytes = Vec::new();
            assert!(stream.read_to_end(&mut bytes).is_ok(), "drain requests");
        },
        |server| {
            let addr = server.addr();
            let done = Gate::new();
            let calls = Rc::new(Cell::new(0));
            let first = Rc::new(Cell::new(None));
            let audit = Rc::new(Report::default());

            let planned = Attempt::new(addr)
                .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Bounded {
                    create: || {
                        let value = Shared::try_copy(b"x").expect("shared request payload");
                        egress::Load::new(value)
                            .try_nonempty()
                            .expect("nonempty request payload")
                    },
                    audit: Rc::clone(&audit),

                    done: done.clone(),
                    left: Cell::new(schedule::LIMIT + 1),
                    calls: Rc::clone(&calls),
                    first: Rc::clone(&first),
                    phase: Cell::new(Phase::New),
                });
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            let run = planned.with_run(
                &mut finish,
                Identity,
                |case| reached(case.try_wait(&done, 1), &audit),
                recovery::Policy::new(|event| reclaim1.push(event), recover),
                leased,
            );
            run.expect("bounded attempt scenario failed");
            assert_reclaimed(&reclaim1);

            let finish = server.finish();
            assert!(finish.is_ok(), "finish held peer");
            audit.no_faults();
            assert_eq!(done.hits(), 1);
            assert!(matches!(first.get(), Some(1..)));
            assert!(calls.get() >= 2);
        },
    )
    .expect("spawn held peer");
}

#[test]
fn exhausted_activations_preserve_liveness_phase() {
    use std::time::{Duration, Instant};

    use dope_core::{driver::route::table::conn::Capacity, io::socket};
    use dope_manifold::{
        app::Host,
        client::{attempt::StreamTarget, conn::engine, session},
        network::client::topology::plan,
    };
    use dope_net::wire::domain::Local;
    use dope_runtime::executor::session::Scope;
    use o3::{base::time::Positive, depot::capacity};

    use crate::common::{
        manifold::client::{connect::open::First, finish::close::members::Until, probe::Probe},
        scenario::{Runtime, case::attempt::Attempts},
    };

    let server = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("bind peer");
    let addr = server.local_addr().expect("peer address");
    let mut finish = Completion::new();
    let mut create = dope_core::driver::life::creation::Attempt::new();
    let layout = plan::Layout::<KeyTag<0>, { schedule::LIMIT + 1 }>::new(schedule::LIMIT + 1)
        .expect("client layout");
    let runtime = Runtime::try_for_plan::<Throughput, _>(&layout).expect("client resources");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    let audit = Rc::new(Report::default());
    let connected = LocalGate::new();
    let expired = Gate::new();
    let redial = Cell::new(None);
    let executor = runtime.try_executor(&mut create).ok().expect("executor");
    executor
                .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(
                    (schedule::LIMIT + 1) as u32,
                )))
                .run(|executor| {
                    let result = executor.try_create(
                        |mut runtime| {
                            let source = runtime.storage();
                            let target = || StreamTarget::new(addr, Default::default());
                            let primary = source.try_reserve().expect("primary attempt").start(target());
                            let hot = Cell::new(None);
                            let ready = Cell::new(None);
                            let running = Cell::new(true);
                            let refills = Cell::new(0);
                            let refill = || {
                                drop(hot.take());
                                if running.get() {
                                    refills.set(refills.get() + 1);
                                    hot.set(Some(source.try_reserve().expect("recycled attempt").start(target())));
                                }
                            };
                            let protocol = session::app::State::<'_, '_, KeyTag<0>, _, First>::try_new(
                                session::Setup::new(
                                    Probe {
                                        audit: audit.clone(),
                                        timeout: Some(Positive::from_millis::<20>()),
                                        peer: |peer: socket::Addr, target| {
                                            assert_eq!(peer.into_std().expect("TCP peer"), addr);
                                            ready.set(Some(target));
                                        },
                                        gate: connected.handle(),
                                        start: Instant::now(),
                                        redial: &redial,
                                        rejected: Some(&refill),
                                        expired: Some(&expired),
                                    },
                                    session::ingress::Plan::none(),
                                ),
                                Capacity::fixed::<{ schedule::LIMIT + 1 }>(),
                                &mut runtime.driver(),
                            )
                            .expect("client session");
                            engine::Startup::new(
                                protocol,
                                source,
                                Capacity::fixed::<{ schedule::LIMIT + 1 }>(),
                                Default::default(),
                                Local::new(()),
                            )
                            .try_with::<Bundle<Tcp, First, Throughput>, _, _>(
                                &mut runtime,
                                Scope::driver,
                                |engine, runtime| {
                                    let now = runtime.driver().driver_ref().clock().now();
                                    runtime.with_at_for(
                                        &mut finish,
                                        Host::new(engine),
                                        now.into(),
                                        Duration::from_secs(2),
                                        |mut app| {
                                            app.drive_for(now.into(), Duration::from_secs(1), Until(|| connected.handle().hits() == 1))
                                                .expect("primary connected")
                                                .into_output();
                                            refill();
                                            ready.get().expect("connection wake").wake();
                                            let result = app.drive_for(
                                                now.clamp_add(Duration::from_millis(20)),
                                                Duration::from_millis(150),
                                                Until(|| {
                                                    if expired.hits() != 1 {
                                                        return false;
                                                    }
                                                    running.set(false);
                                                    drop(hot.take());
                                                    true
                                                }),
                                            );
                                            running.set(false);
                                            drop(hot.take());
                                            assert!(refills.get() >= schedule::LIMIT);
                                            assert_eq!(expired.hits(), 1, "zero-budget activations preserve the pending liveness phase");
                                            assert!(result.is_ok(), "inbound timeout progresses");
                                        },
                                    ).expect("application teardown");
                                },
                            )
                            .expect("connector installation");
                            primary.cancel();
                        },
                        recovery::Policy::new(|event| reports.push(event), recover),
                    );
                    result.as_ref().map_err(Kind::from).expect("attempt source");
                });
    assert!(matches!(reports.events(), Ok(events) if events.is_empty()));
    audit.checked((0, 0));
    let stream = server
        .accept_for(GUARD)
        .expect("accept held peer")
        .expect("connected peer");
    stream.read_wait(Some(GUARD)).expect("peer read deadline");
    assert_eq!(stream.read(&mut [0]).expect("close held connection"), 0);
}

#[test]
fn exhaustion_waits() {
    for cause in [
        opening::Exhaustion::Submission(SubmitError::from(Vacancy::IdentityExhausted)),
        opening::Exhaustion::Identity,
    ] {
        let mut finish = Completion::new();
        let listener =
            peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let conn_g = LocalGate::new();
        let conn = conn_g.handle();
        let begin_g = LocalGate::new();
        let step_g = LocalGate::new();
        let close_g = LocalGate::new();
        let closed = close_g.handle();
        let notice_g = LocalGate::new();
        let notice = notice_g.handle();
        let events = Trace::<Exhaustion, 2>::new();
        let audit = Rc::new(Report::default());
        let planned = Connector::<2>::new(addr, BACKOFF, BACKOFF)
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Value::new(Redial {
                audit: Rc::clone(&audit),
                kind: Mode::Exhausted(cause),
                conn,
                begun: begin_g.handle(),
                steps: step_g.handle(),
                closed,
                exhausted: Some((notice, &events)),
                pending: Cell::new(false),
            }))
            .expect("prepare two live connections");
        let mut reclaim = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);
        planned
            .with_run(
                &mut finish,
                |case| {
                    let mut held = [const { None }; 2];
                    for (index, slot) in held.iter_mut().enumerate() {
                        reached(case.try_wait(&conn, index as u32 + 1), &audit);
                        *slot = Some(
                            listener
                                .accept_for(GUARD)
                                .expect("accept")
                                .expect("connected peer"),
                        );
                    }
                    reached(case.try_wait(&closed, 1), &audit);
                    case.try_pause(BACKOFF.saturating_mul(4))
                        .expect("retain the healthy connection");
                    audit.no_faults();
                    assert_eq!((conn.hits(), closed.hits(), notice.hits()), (2, 1, 0));

                    drop(held);
                    reached(case.try_wait(&notice, 1), &audit);
                    case.try_pause(BACKOFF.saturating_mul(4))
                        .expect("observe terminal admission");
                },
                recovery::Policy::new(|event| reclaim.push(event), recover),
                native,
            )
            .expect("identity exhaustion scenario");
        assert_reclaimed(&reclaim);
        audit.no_faults();
        assert_eq!((conn.hits(), closed.hits(), notice.hits()), (2, 2, 1));
        assert_eq!(
            &*events.values().expect("exhaustion observation"),
            &[(cause, 2, 2)]
        );
        assert!(listener.try_accept().expect("accept check").is_none());
    }
}

#[test]
fn close_policy_controls_redial() {
    for kind in [Mode::Reconnect, Mode::Permanent] {
        let mut finish = Completion::new();
        let listener =
            peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let mut held = [const { None }; 2];
        let conn_g = LocalGate::new();
        let conn = conn_g.handle();
        let begin_g = LocalGate::new();
        let begun = begin_g.handle();
        let step_g = LocalGate::new();
        let steps = step_g.handle();
        let close_g = LocalGate::new();
        let closed = close_g.handle();
        let audit = Rc::new(Report::default());

        let planned = Connector::<1>::new(addr, BACKOFF, BACKOFF)
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Value::new(Redial {
                audit: Rc::clone(&audit),

                kind,
                conn,
                begun,
                steps,
                closed,
                exhausted: None,
                pending: Cell::new(false),
            }))
            .expect("prepare actual client layout");
        let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

        planned
            .with_run(
                &mut finish,
                |case| {
                    reached(case.try_wait(&conn, 1), &audit);
                    held[0] = Some(
                        listener
                            .accept_for(GUARD)
                            .expect("accept")
                            .expect("connected peer"),
                    );
                    reached(case.try_wait(&closed, 1), &audit);
                    if matches!(kind, Mode::Reconnect) {
                        reached(case.try_wait(&conn, 2), &audit);
                        held[1] = Some(
                            listener
                                .accept_for(GUARD)
                                .expect("accept")
                                .expect("connected peer"),
                        );
                    } else {
                        case.try_pause(BACKOFF.saturating_mul(4))
                            .expect("runtime pause");
                    }
                },
                recovery::Policy::new(|event| reclaim1.push(event), recover),
                native,
            )
            .expect("application scenario");
        assert_reclaimed(&reclaim1);

        let expected = usize::from(matches!(kind, Mode::Reconnect)) + 1;
        let accepts = held.iter().flatten().count();
        assert!(listener.try_accept().expect("accept check").is_none());
        audit.no_faults();
        assert_eq!(
            (audit.sent.get(), audit.cancelled.get()),
            (Some(0), Some(0))
        );
        assert_eq!(conn.hits(), expected as u32);
        assert_eq!(steps.hits(), begun.hits() + 1);
        assert_eq!(closed.hits(), expected as u32);
        assert_eq!(accepts, expected);
        if matches!(kind, Mode::Permanent) {
            assert_eq!((begun.hits(), steps.hits()), (1, 2));
        }
        drop(held);
    }
}

#[test]
fn close_yield_resumes() {
    let mut finish = Completion::new();
    let server = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("bind peer");
    let addr = server.local_addr().expect("peer address");
    let conn = Gate::new();
    let closed = Gate::new();
    let retired = Gate::new();
    let checks = Rc::new(Cell::new(0));
    let resumed = Rc::new(Cell::new(false));
    let audit = Rc::new(Report::default());

    let planned = Attempt::new(addr)
        .stream(StreamConfig {
            no_delay: Some(option::Switch::On),
            ..Default::default()
        })
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Yielding {
            audit: Rc::clone(&audit),

            life: Life {
                conn: conn.clone(),
                closed: closed.clone(),
                retired: retired.clone(),
            },
            pending: Cell::new(false),
            yielded: Cell::new(false),
            checks: Rc::clone(&checks),
            resumed: Rc::clone(&resumed),
        });
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            Identity,
            |case| {
                reached(case.try_wait(&conn, 1), &audit);
                reached(case.try_wait(&closed, 1), &audit);
                reached(case.try_wait(&retired, 1), &audit);
            },
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            leased,
        )
        .expect("yielding attempt scenario failed");
    assert_reclaimed(&reclaim1);

    audit.no_faults();
    assert_eq!(
        (audit.sent.get(), audit.cancelled.get()),
        (Some(0), Some(0))
    );
    assert_eq!((conn.hits(), closed.hits(), retired.hits()), (1, 1, 1));
    assert!(resumed.get());
    assert_eq!(checks.get(), 2);
    let stream = server
        .accept_for(GUARD)
        .expect("accept held peer")
        .expect("connected peer");
    stream.read_wait(Some(GUARD)).expect("peer read deadline");
    assert_eq!(stream.read(&mut [0]).expect("wait for close"), 0);
}
fn reached(wait: Result<fibers::Wait, fibers::Run>, audit: &Report<error::Error, 16>) {
    audit.no_faults();
    assert!(
        matches!(wait, Ok(fibers::Wait::Reached)),
        "progress: {wait:?}"
    );
}
