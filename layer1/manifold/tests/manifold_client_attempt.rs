pub mod common;

use std::{convert::Infallible, ptr, rc::Rc, time::Duration};

use dope_core::driver::{
    self,
    life::{creation::Attempt, recovery},
    route::{Id, KeyTag, table::conn::Capacity},
    schedule::clock,
};
use dope_manifold::{
    app::{Host, creation, env::Bundle},
    client::{
        attempt::{StreamTarget, queue},
        conn::engine,
    },
    timing::Throughput,
};
use dope_net::{
    link::egress,
    tcp::Tcp,
    wire::{Identity, domain::Local},
};
use dope_runtime::{
    executor::session::{Completion, Scope},
    topology::error::Kind,
};
use dope_test::{
    alloc::Tracker,
    reclaim::Reports,
    scope::{self, Driver},
};
use o3::depot::{arena::slab::Vacancy, capacity};

use crate::common::{
    contract::Report,
    manifold::client::{
        connect::dial::{
            Cancellation, Stage,
            empty::{Empty, Trace},
        },
        error,
    },
    scenario::{Runtime, case::attempt::Attempts, ports::Loopback},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reserves_independently() {
    let mut create = Attempt::new();
    let mut reports = Vec::new();
    let result = Runtime::throughput().try_executor(&mut create);
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor construction");
    let Ok(executor) = result else {
        return;
    };
    executor
        .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(2)))
        .run(|executor| {
            ((executor.try_create(
                |session| {
                    let queue = session.storage();
                    let ((), alloc) = Tracker::usage(|| {
                        let (second, key) = {
                            let first = queue.try_reserve().expect("first reservation");
                            let second = queue.try_reserve().expect("independent reservation");
                            assert!(!queue.has_room());
                            assert!(matches!(queue.try_reserve(), Err(Vacancy::Full)));
                            (second, first.id())
                        };
                        let first = queue.try_reserve().expect("reservation returns capacity");
                        assert!(first.id() != key);
                        let key = first.id();
                        let second = {
                            let first = first.start(Loopback(8994).target());
                            assert!(first.id() == key);
                            second.start(Loopback(8995).target())
                        };
                        let next = queue.try_reserve().expect("lease returns only its slot");
                        assert!(next.id().index() == key.index());
                        assert!(next.id() != key);
                        second.cancel();
                        assert!(queue.has_room());
                    });
                    assert_eq!(alloc, (0, 0));
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            ))
            .as_ref()
            .map_err(Kind::from)
            .expect("attempt source generation and finalization"));
        });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn capacity_mismatch() {
    let audit = Rc::new(Report::default());
    let trace = Rc::new(Trace::default());
    let mut create = Attempt::new();
    let mut reports = Vec::new();
    let result = Runtime::throughput().try_executor(&mut create);
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor construction");
    let Ok(executor) = result else {
        return;
    };
    executor
        .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
        .run(|executor| {
            ((executor.try_create(
                |mut session| {
                    let source = session.storage();
                    let result = engine::Startup::new(
                        Empty {
                            audit: Rc::clone(&audit),
                            trace: Rc::clone(&trace),
                            reject: None,
                        },
                        source,
                        Capacity::fixed::<2>(),
                        Default::default(),
                        Local::new(Identity),
                    )
                    .try_with::<Bundle<Tcp, Identity, Throughput>, _, _>(
                        &mut session.driver(),
                        driver::Context::reborrow,
                        |_, _| (),
                    );
                    let (error,) = match result {
                        Err(creation::Error::Attempt(error)) => Some((error,)),
                        _ => None,
                    }
                    .expect("attempt capacity error");
                    assert!((error.requested()) == (capacity::Count::new(2)));
                    assert!((error.available()) == (capacity::Count::new(1)));
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            ))
            .as_ref()
            .map_err(Kind::from)
            .expect("attempt source generation and finalization"));
        });

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    checked(&audit, &trace);
}

#[test]
fn cancel_before_reuse() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let runtime = Runtime::throughput();
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    let trace = Rc::new(Trace::default());
    let audit = Rc::new(Report::default());
    let result = runtime.try_executor(&mut create);
    result
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor construction");
    let Ok(executor) = result else {
        return;
    };
    executor
        .with_plan(Attempts::<KeyTag<0>>::new(capacity::Count::new(1)))
        .run(|executor| {
            ((executor.try_create(
                |mut sess| {
                    let source = sess.storage();
                    let reserve = (source.try_reserve()).expect("first attempt");
                    let lease =
                        reserve.start(StreamTarget::new(Loopback(9).addr(), Default::default()));
                    let result = engine::Startup::new(
                        Empty {
                            audit: Rc::clone(&audit),
                            trace: Rc::clone(&trace),
                            reject: None,
                        },
                        source,
                        Capacity::fixed::<1>(),
                        Default::default(),
                        Local::new(Identity),
                    )
                    .try_with::<Bundle<Tcp, Identity, Throughput>, _, _>(
                        &mut sess,
                        Scope::driver,
                        |core, sess| {
                            let start =
                                clock::Deadline::from(sess.driver().driver_ref().clock().now());
                            let result = sess.with_at_for(
                                &mut finish,
                                Host::new(core),
                                start,
                                Duration::from_secs(2),
                                |mut app| {
                                    let fixed = app.drive(
                                        start,
                                        Cancellation {
                                            state: Stage::Pending(lease),
                                        },
                                    );
                                    let fixed = (fixed).expect("cancellation drive");
                                    let key = fixed
                                        .into_output()
                                        .expect("cancellation polled only until complete");
                                    assert_eq!(trace.connected.get(), 0);
                                    assert!(
                                        trace
                                            .failed
                                            .values()
                                            .expect("connection failures")
                                            .is_empty()
                                    );
                                    assert_eq!(trace.closed.get(), 0);
                                    assert_eq!(trace.drained.get(), 0);
                                    assert!((trace.reason.get()).is_none());
                                    assert!(source.has_room(), "cancelled generation must retire");
                                    let next =
                                        (source.try_reserve()).expect("reused attempt capacity");
                                    let next = next.start(StreamTarget::new(
                                        Loopback(9).addr(),
                                        Default::default(),
                                    ));
                                    assert!(next.id() != key);
                                },
                            );
                            assert!(result.is_ok(), "manual application teardown");
                        },
                    );
                    assert!(result.is_ok(), "build connector machine");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            ))
            .as_ref()
            .map_err(Kind::from)
            .expect("attempt source generation and finalization"));
        });

    assert!(
        matches!(reports.events(), Ok(events) if events.is_empty()),
        "unexpected reclaimed operations: {reports:?}"
    );
    checked(&audit, &trace);
}

#[test]
fn rejects_recreated_source() {
    let audit = Rc::new(Report::default());
    let trace = Rc::new(Trace::default());
    use queue::mailbox::{Cancellation, Conflict, Domain, Error};
    let mut reports = Vec::new();

    Driver::new(Runtime::throughput().config()).with_scope(
        |scope| {
            let first = (Domain::<Tcp, u64, Box<u64>>::try_new(
                capacity::Count::new(1),
                &mut scope.context().storage,
            ))
            .expect("first mailbox source");
            let lease = (first.try_reserve()).expect("first attempt").start(
                11,
                StreamTarget::new(Loopback(0).addr(), Default::default()),
            );
            let receipt = lease.receipt();
            (engine::Startup::new(
                Empty {
                    audit: Rc::clone(&audit),
                    trace: Rc::clone(&trace),
                    reject: None,
                },
                first.source(),
                Capacity::fixed::<1>(),
                egress::Config::default(),
                Local::new(Identity),
            )
            .try_with::<Bundle<Tcp, Identity, Throughput>, _, _>(
                scope,
                |scope| scope.context(),
                |engine, _| drop(engine),
            ))
            .expect("first source engine");
            scope.quiesce();
            let second = (Domain::<Tcp, u64, Box<u64>>::try_new(
                capacity::Count::new(1),
                &mut scope.context().storage,
            ))
            .expect("next mailbox source");
            let next = (second.try_reserve()).expect("next attempt").start(
                22,
                StreamTarget::new(Loopback(0).addr(), Default::default()),
            );
            assert!(lease.id() == next.id());

            let outcome = Box::new(91);
            let pointer = ptr::from_ref(outcome.as_ref());
            let (result, allocs) = Tracker::usage(|| second.try_settle(&receipt, outcome));
            assert_eq!(allocs, (0, 0));
            let (cause, outcome) = result.err().expect("foreign source must reject outcome");
            assert!(matches!(cause, Error::Slot(Conflict::Foreign(key)) if key == lease.id()));
            assert_eq!(ptr::from_ref(outcome.as_ref()), pointer);
            assert!(matches!(
                first.try_settle(&next.receipt(), Box::new(92)),
                Err((Error::Slot(Conflict::Foreign(_)), _))
            ));
            assert!(matches!(
                first.try_settle(&receipt, outcome),
                Ok((11, None))
            ));
            assert!(matches!(
                second.try_settle(&next.receipt(), Box::new(92)),
                Ok((22, None))
            ));
            assert!(matches!(lease.cancel(), Cancellation::Settled(value) if *value == 91));
            assert!(matches!(next.cancel(), Cancellation::Settled(value) if *value == 92));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("native lifecycle");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    checked(&audit, &trace);
}

fn checked(audit: &Report<error::Error, 16>, trace: &Trace) {
    audit.checked((0, 0));
    let failures = trace.failed.values().expect("connection failures");
    assert!(failures.is_empty(), "connection failures: {failures:?}");
    assert_eq!(
        [
            trace.prepared.get(),
            trace.connected.get(),
            trace.closed.get(),
            trace.drained.get()
        ],
        [0; 4]
    );
    assert!(trace.reason.get().is_none());
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
