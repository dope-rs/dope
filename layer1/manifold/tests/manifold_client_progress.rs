pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    io::{self, Read as _},
    rc::Rc,
};

use dope_core::{
    driver::{
        life::recovery,
        result::SubmitError,
        route::{Id, KeyTag},
        schedule,
    },
    io::socket::option,
    platform::wake,
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
        peer::{self, AcceptScript, TcpScript},
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
            redial::{BACKOFF, Exhaustion, MAX, Mode, Redial, Run},
            yielding::{Life, Yielding},
        },
    },
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
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            let finish = server.finish();
            assert!(finish.is_ok(), "finish held peer");
            checked(&audit);
            assert_eq!(done.hits(), 1);
            assert_eq!(first.get(), Some(1));
            assert!(calls.get() >= 2);
        },
    )
    .expect("spawn held peer");
}

#[test]
fn reconnect_redials() {
    let run = run(Mode::Reconnect).expect("spawn held peer");
    assert_eq!(run.conns, 2);
    assert_eq!(run.steps, run.begun + 1);
    assert_eq!(run.closed, 2);
    assert_eq!(run.accepts, 2);
}

#[test]
fn permanent_stops() {
    let run = run(Mode::Permanent).expect("spawn held peer");
    assert_eq!(run.conns, 1);
    assert_eq!(run.begun, 1);
    assert_eq!(run.steps, 2);
    assert_eq!(run.closed, 1);
    assert_eq!(run.accepts, 1);
}

#[test]
fn exhaustion_waits() {
    let mut finish = Completion::new();
    let cause = SubmitError::from(Vacancy::IdentityExhausted);
    let (wait, release) = wake::Ends::blocking().expect("held peer release").split();
    AcceptScript::with(
        move |acceptor| {
            let held = [acceptor.accept()?, acceptor.accept()?];
            assert!(wait.wait_timeout(GUARD).is_ok(), "release held peers");
            drop(held);
            let mut extra = Vec::new();
            loop {
                match acceptor.accept() {
                    Ok(stream) => extra.push(stream),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        return Ok(2 + extra.len());
                    }
                    Err(error) => return Err(error),
                }
            }
        },
        |server| {
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
            let planned = Connector::<2>::new(server.addr(), BACKOFF, BACKOFF)
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
                        reached(case.try_wait(&conn, 2), &audit);
                        reached(case.try_wait(&closed, 1), &audit);
                        case.try_pause(BACKOFF.saturating_mul(4))
                            .expect("retain the healthy connection");
                        checked(&audit);
                        assert_eq!((conn.hits(), closed.hits(), notice.hits()), (2, 1, 0));

                        release.notify().expect("retire the remaining peer");
                        reached(case.try_wait(&notice, 1), &audit);
                        case.try_pause(BACKOFF.saturating_mul(4))
                            .expect("observe terminal admission");
                    },
                    recovery::Policy::new(|event| reclaim.push(event), recover),
                    native,
                )
                .expect("identity exhaustion scenario");
            assert!(
                matches!(reclaim.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim:?}"
            );
            checked(&audit);
            assert_eq!((conn.hits(), closed.hits(), notice.hits()), (2, 2, 1));
            assert_eq!(
                &*events.values().expect("exhaustion observation"),
                &[(opening::Exhaustion::Submission(cause), 2, 2)]
            );
            assert_eq!(
                server
                    .stop()
                    .expect("finish held peer")
                    .expect("accept connections"),
                2
            );
        },
    )
    .expect("spawn held peers");
}

fn run(kind: Mode) -> Result<Run, peer::Spawn> {
    let mut finish = Completion::new();
    let (accepts, tx) = wake::Ends::blocking().map_err(peer::Spawn::Wake)?.split();
    AcceptScript::with(
        move |acceptor| {
            let mut held = Vec::new();
            loop {
                match acceptor.accept() {
                    Ok(stream) => {
                        held.push(stream);
                        tx.notify().expect("accept observer remains alive");
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        return Ok(held.len());
                    }
                    Err(error) => return Err(error),
                }
            }
        },
        |server| {
            let addr = server.addr();
            let conn_g = LocalGate::new();
            let conn = conn_g.handle();
            let begin_g = LocalGate::new();
            let begun = begin_g.handle();
            let step_g = LocalGate::new();
            let steps = step_g.handle();
            let close_g = LocalGate::new();
            let closed = close_g.handle();
            let audit = Rc::new(Report::default());

            let planned = Connector::<MAX>::new(addr, BACKOFF, BACKOFF)
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
                        reached(case.try_wait(&closed, 1), &audit);
                        if matches!(kind, Mode::Reconnect) {
                            reached(case.try_wait(&conn, 2), &audit);
                        } else {
                            case.try_pause(BACKOFF.saturating_mul(4))
                                .expect("runtime pause");
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

            let expected = usize::from(matches!(kind, Mode::Reconnect)) + 1;
            for _ in 0..expected {
                assert!(
                    accepts.wait_timeout(GUARD).is_ok(),
                    "peer accepts each connected socket"
                );
            }
            let accepts = server
                .stop()
                .expect("finish held peer")
                .expect("accept held connector");
            checked(&audit);
            assert_eq!(
                (audit.sent.get(), audit.cancelled.get()),
                (Some(0), Some(0))
            );
            Run {
                conns: conn.hits(),
                begun: begun.hits(),
                steps: steps.hits(),
                closed: closed.hits(),
                accepts,
            }
        },
    )
}

#[test]
fn close_yield_resumes() {
    let mut finish = Completion::new();

    TcpScript::with(
        |stream| {
            let mut bytes = Vec::new();
            assert!(stream.read_to_end(&mut bytes).is_ok(), "wait for close");
        },
        |server| {
            let addr = server.addr();
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
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            let finish = server.finish();
            assert!(finish.is_ok(), "finish held peer");
            checked(&audit);
            assert_eq!(
                (audit.sent.get(), audit.cancelled.get()),
                (Some(0), Some(0))
            );
            assert_eq!((conn.hits(), closed.hits(), retired.hits()), (1, 1, 1));
            assert!(resumed.get());
            assert_eq!(checks.get(), 2);
        },
    )
    .expect("spawn held peer");
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn leased<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>, lease: Option<bool>) -> T {
    let output = native(result);
    assert_eq!(lease, Some(true), "terminal attempt lease: {output:?}");
    output
}

fn checked(audit: &Report<error::Error, 16>) {
    let faults = audit.faults.values().expect("fault observations");
    let result = error::Error::check(&faults);
    assert!(result.is_ok(), "{result:?}");
}

fn reached(wait: Result<fibers::Wait, fibers::Run>, audit: &Report<error::Error, 16>) {
    checked(audit);
    assert!(
        matches!(wait, Ok(fibers::Wait::Reached)),
        "progress: {wait:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
