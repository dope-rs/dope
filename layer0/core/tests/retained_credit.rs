use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::mem;

use dope_core::driver::{
    receive::credit::{Take, Wake, error::Arm},
    route::{Epoch, KeyTag, indices::SlotIndex, table::conn},
    schedule::ready::Readiness,
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn reuses_quiescent_storage() {
    const CAPACITY: usize = 17;
    const BACKING: usize = 32;
    const HEADER_BYTES: usize = 6 * mem::size_of::<usize>();
    const RESIDENT_BYTES: usize = 2 * mem::size_of::<usize>();
    let capacity = conn::Capacity::fixed::<CAPACITY>();
    let mut reports = Vec::new();

    Runtime::throughput().with_driver(
        |driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(target)).expect("ready slot");

            let control = {
                let (storage, growth) = Tracker::usage(|| site.scheduler().try_storage(capacity));
                let storage = (storage).expect("initial retained storage");
                assert_eq!(
                    growth,
                    (1, HEADER_BYTES + BACKING * RESIDENT_BYTES),
                    "one group header and rounded two-word residents",
                );

                let slot = (storage.get(SlotIndex::ZERO)).expect("credit control");
                let control = slot.control();
                let ((), hot) = Tracker::usage(|| {
                    let pending = (control.arm(target, ready.target())).expect("idle credit");
                    assert!((pending.id().slot()) == (SlotIndex::ZERO));
                    assert!(matches!(
                        control.arm(target, ready.target()),
                        Err(Arm::Busy)
                    ));
                    {
                        let returned = pending.claim();
                        assert!((returned.id().slot()) == (SlotIndex::ZERO));
                    }
                    assert!(matches!(slot.take(), Take::Ready(Wake::Returned)));

                    let retry = (control.arm(target, ready.target())).expect("returned credit");
                    retry.claim().retry();
                    assert!(matches!(slot.take(), Take::Ready(Wake::Retry)));

                    {
                        let armed = (control.arm(target, ready.target())).expect("retried credit");
                        assert!((armed.id().slot()) == (SlotIndex::ZERO));
                    }
                    assert!(!slot.is_active());
                    assert!(matches!(slot.take(), Take::Idle));
                });
                assert_eq!(hot, (0, 0));
                control
            };

            {
                let (reused, reuse) = Tracker::usage(|| site.scheduler().try_storage(capacity));
                let reused = (reused).expect("reused retained storage");
                assert_eq!(
                    reuse,
                    (0, 0),
                    "the quiescent owner group is reactivated in place",
                );
                assert!(reused.get(SlotIndex::ZERO).is_ok());
                assert!(
                    matches!(control.arm(target, ready.target()), Err(Arm::Stale(_))),
                    "old generation must be stale",
                );
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn owner_outlives_storage() {
    let capacity = conn::Capacity::fixed::<1>();
    let mut reports = Vec::new();

    Runtime::throughput().with_driver(
        |driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(target)).expect("ready slot");

            let (stale, held) = {
                let storage =
                    (site.scheduler().try_storage(capacity)).expect("initial retained storage");
                let slot = (storage.get(SlotIndex::ZERO)).expect("credit control");
                let stale = slot.control();
                let held = (stale.arm(target, ready.target())).expect("idle credit");
                let held = held.claim();
                assert!((held.id().slot()) == (SlotIndex::ZERO));
                (stale, held)
            };

            let ((), release) = Tracker::usage(|| {
                let returned = held;
                assert!((returned.id().slot()) == (SlotIndex::ZERO));
            });
            assert_eq!(release, (0, 0));

            {
                let (reused, reuse) = Tracker::usage(|| site.scheduler().try_storage(capacity));
                let reused = (reused).expect("reused retained storage");
                assert_eq!(reuse, (0, 0));
                assert!(reused.get(SlotIndex::ZERO).is_ok());
                assert!(
                    matches!(stale.arm(target, ready.target()), Err(Arm::Stale(_))),
                    "old owner must be stale",
                );
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn take_keeps_ready() {
    let mut reports = Vec::new();
    Runtime::throughput().with_driver(
        |driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = site.ready().make_slot(target).expect("ready slot");
            let store = site
                .scheduler()
                .try_storage(conn::Capacity::fixed::<1>())
                .expect("credit storage");
            let slot = store.get(SlotIndex::ZERO).expect("credit slot");
            let control = slot.control();
            for retry in [false, true] {
                let held = control
                    .arm(target, ready.target())
                    .expect("idle credit")
                    .claim();
                ready.activate();
                if retry {
                    held.retry();
                } else {
                    drop(held);
                }
                assert!(matches!(
                    (retry, slot.take()),
                    (false, Take::Ready(Wake::Returned)) | (true, Take::Ready(Wake::Retry))
                ));
                assert!(matches!(slot.take(), Take::Idle));
                assert!(!slot.is_active());
                assert!(matches!(ready.take(), Readiness::Ready));
                assert!(matches!(ready.take(), Readiness::Idle));
            }
            let pending = control.arm(target, ready.target()).expect("idle credit");
            ready.activate();
            pending.cancel();
            assert!(matches!(slot.take(), Take::Idle));
            assert!(!slot.is_active());
            assert!(matches!(ready.take(), Readiness::Ready));
            assert!(matches!(ready.take(), Readiness::Idle));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn revoke_keeps_ready() {
    let mut reports = Vec::new();
    Runtime::throughput().with_driver(
        |driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = site.ready().make_slot(target).expect("ready slot");
            for retry in [false, true] {
                let store = site
                    .scheduler()
                    .try_storage(conn::Capacity::fixed::<1>())
                    .expect("credit storage");
                let slot = store.get(SlotIndex::ZERO).expect("credit slot");
                let control = slot.control();
                let held = control
                    .arm(target, ready.target())
                    .expect("idle credit")
                    .claim();
                ready.activate();
                if retry {
                    held.retry();
                } else {
                    drop(held);
                }
                assert!(slot.is_active());
                assert!(!slot.is_held());
                drop(store);
                assert!(matches!(
                    control.arm(target, ready.target()),
                    Err(Arm::Stale(_))
                ));
                assert!(matches!(ready.take(), Readiness::Ready));
                assert!(matches!(ready.take(), Readiness::Idle));
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn detach_keeps_ready() {
    let mut reports = Vec::new();
    Runtime::throughput().with_scope(
        |scope| {
            let site = scope.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = site.ready().make_slot(target).expect("ready slot");
            for retry in [false, true] {
                let store = site
                    .scheduler()
                    .try_storage(conn::Capacity::fixed::<1>())
                    .expect("credit storage");
                let slot = store.get(SlotIndex::ZERO).expect("credit slot");
                let control = slot.control();
                let held = control
                    .arm(target, ready.target())
                    .expect("idle credit")
                    .claim();
                if retry {
                    held.retry();
                } else {
                    drop(held);
                }
                assert!(slot.is_active());
                assert!(!slot.is_held());
                scope.finish(false, |_, last| {
                    ready.activate();
                    last.credit().detach(&store);
                    assert!(matches!(slot.take(), Take::Idle));
                    assert!(!slot.is_active());
                    assert!(matches!(
                        control.arm(target, ready.target()),
                        Err(Arm::Detached)
                    ));
                    assert!(matches!(ready.take(), Readiness::Ready));
                    assert!(matches!(ready.take(), Readiness::Idle));
                });
                drop(store);
                assert!(matches!(
                    control.arm(target, ready.target()),
                    Err(Arm::Stale(_))
                ));
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
