pub mod common;

use o3::depot::arena::slab::Vacancy;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{convert::Infallible, mem};

use dope_core::driver::{
    life::{creation::Attempt, domain, recovery},
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::wake::Waker,
    settings::claim::Needs,
    storage::Create as _,
};
use dope_manifold::{app::creation, client::attempt::queue, timing::Throughput};
use dope_net::tcp;
use dope_runtime::executor::{Executor, session};
use dope_test::{
    alloc::Tracker,
    harness::observe::Trace,
    scope::{self, Driver},
};
use o3::depot::capacity::Count;
use queue::mailbox::{Cancellation, Conflict, Error, Poll, Vacant};

use crate::common::scenario::{case::attempt::Mailbox, ports::Loopback};

#[test]
fn avoids_allocation() {
    let mut reports = Vec::new();
    let plan = Mailbox::<u64>::new(Count::new(1));
    let config = (Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Throughput>()))
        .expect("mailbox requirements");
    Driver::new(config).with_scope(
        |scope| {
            let mailbox = plan
                .try_build(scope.context().storage)
                .expect("mailbox source");
            let driver = scope.driver_ref();
            let dispatch = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (driver.ready().make_slot(dispatch)).expect("ready slot");
            let ((), cost) = Tracker::usage(|| {
                for value in 0..10_000 {
                    let vacant =
                        (mailbox.try_reserve()).expect("released generation must remain reusable");
                    let lease = vacant.start(value, Loopback(0).target());
                    let done = lease.receipt();
                    let lease = match lease.poll(Waker::from(ready.target())) {
                        Poll::Pending(lease) => Some(lease),
                        _ => None,
                    }
                    .expect("pending attempt");
                    let (context, waiter) = match (mailbox.try_settle(&done, value + 1))
                        .expect("exact pending generation")
                    {
                        (context, Some(waiter)) => Some((context, waiter)),
                        _ => None,
                    }
                    .expect("exact pending generation");
                    assert_eq!(context, value);
                    waiter.wake();
                    let (outcome, lease) = match lease.poll(Waker::from(ready.target())) {
                        Poll::Ready(outcome, lease) => Some((outcome, lease)),
                        _ => None,
                    }
                    .expect("completed attempt");
                    assert_eq!(outcome, value + 1);
                    lease.cancel();
                }
            });
            assert_eq!(cost, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("driver lifecycle");
        },
    );

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn retains_lease_layout() {
    assert_eq!(
        mem::size_of::<Option<Vacant<'static, 'static, tcp::Tcp, u64, u64>>>(),
        mem::size_of::<Vacant<'static, 'static, tcp::Tcp, u64, u64>>(),
    );
}

#[test]
fn returns_reservations() {
    let mut reports = Vec::new();
    let plan = Mailbox::<u64>::new(Count::new(1));
    let config = (Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Throughput>()))
        .expect("mailbox requirements");
    Driver::new(config).with_scope(
        |scope| {
            let mailbox = plan
                .try_build(scope.context().storage)
                .expect("mailbox source");
            let vacant = (mailbox.try_reserve()).expect("initial vacancy");
            assert!(!mailbox.source().has_room());
            assert!(matches!(
                mailbox.try_reserve(),
                Err(Error::Queue(Vacancy::Full))
            ));
            drop(vacant);
            assert!(mailbox.source().has_room());
            let vacant = (mailbox.try_reserve()).expect("returned vacancy");
            let lease = vacant.start(7, Loopback(0).target());
            let receipt = lease.receipt();
            assert!(matches!(mailbox.try_settle(&receipt, 91), Ok((7, None))));
            assert!(matches!(lease.cancel(), Cancellation::Settled(91)));
            assert!(mailbox.source().has_room());
            let vacant = (mailbox.try_reserve()).expect("cancelled vacancy");
            let (cause, value) = mailbox
                .try_settle(&receipt, 17)
                .err()
                .expect("unpublished generation rejects a stale receipt");
            assert!(matches!(cause, Error::Slot(Conflict::Busy(key)) if key == receipt.id()));
            assert_eq!(value, 17);
            assert!(matches!(
                mailbox.try_reserve(),
                Err(Error::Queue(Vacancy::Full))
            ));
            drop(vacant);
            let vacant = (mailbox.try_reserve()).expect("released busy reservation");
            let lease = vacant.start(8, Loopback(0).target());
            assert!(lease.id() != receipt.id());
            assert!(matches!(
                mailbox.try_settle(&lease.receipt(), value),
                Ok((8, None))
            ));
            assert!(matches!(lease.cancel(), Cancellation::Settled(17)));
            let vacant = (mailbox.try_reserve()).expect("settled generation released");
            drop(vacant.start(8, Loopback(0).target()));
            assert!(mailbox.source().has_room());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("driver lifecycle");
        },
    );

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn wakes_latest() {
    let mut finish = session::Completion::new();
    let mut reports = Vec::new();
    let plan = Mailbox::<u64>::new(Count::new(1));
    let config = (Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Throughput>()))
        .expect("mailbox requirements");
    let mut create = Attempt::new();
    let executor = {
        let result = Executor::new(&mut create, config);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    };
    executor.with_plan(plan).run(|executor| {
        prepared(executor.try_create(
            |mut scope| {
                let mailbox = scope.storage();
                let driver = scope.driver().driver_ref();
                let first = driver
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch();
                let second = driver
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                    .dispatch();
                let earlier = (driver.ready().make_slot(first)).expect("first ready slot");
                let latest = (driver.ready().make_slot(second)).expect("second ready slot");
                let vacant = (mailbox.try_reserve()).expect("attempt vacancy");
                let lease = vacant.start(9, Loopback(0).target());
                let done = lease.receipt();
                let lease = match lease.poll(Waker::from(earlier.target())) {
                    Poll::Pending(lease) => Some(lease),
                    _ => None,
                }
                .expect("first pending registration");
                let lease = match lease.poll(Waker::from(latest.target())) {
                    Poll::Pending(lease) => Some(lease),
                    _ => None,
                }
                .expect("replacement pending registration");
                let waiter = match (mailbox.try_settle(&done, 91))
                    .expect("settlement must retain the latest registration and context")
                {
                    (9, Some(waiter)) => Some(waiter),
                    _ => None,
                }
                .expect("settlement must retain the latest registration and context");
                waiter.wake();
                let tokens =
                    Trace::<_, 1>::try_drain(&mut finish, &mut scope).expect("ready dispatch");
                assert!(tokens.matches(&[second]).expect("ready dispatch trace"));
                assert!(matches!(lease.cancel(), Cancellation::Settled(91)));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        ));
    });

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn prepared<O>(result: Result<(), domain::Error<'_, domain::Rejected<creation::Error, O>, ()>>) {
    result
        .as_ref()
        .map_err(|error| error.cause().map_storage(|rejected| &rejected.cause))
        .expect("native storage");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
