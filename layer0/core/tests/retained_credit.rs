pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    receive::credit::{Take, Wake, error::Arm},
    route::{Epoch, KeyTag, indices::SlotIndex, table::conn},
    schedule::ready::Readiness,
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn take_keeps_ready() {
    Runtime::throughput().run(|scope| {
        let driver = scope.context();
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
        let activity = Tracker::begin();
        let pending = slot.arm(target, ready.target()).expect("idle credit");
        assert!(pending.id().slot() == SlotIndex::ZERO);
        let held = pending.claim();
        assert!(held.id().slot() == SlotIndex::ZERO);
        drop(held);
        assert!(matches!(slot.take(), Take::Ready(Wake::Returned)));
        assert!(matches!(ready.take(), Readiness::Ready));
        assert!(matches!(ready.take(), Readiness::Idle));
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
        assert_eq!(activity.finish(), [0; 4].into());
    });
}

#[test]
fn revoke_keeps_ready() {
    Runtime::throughput().run(|scope| {
        let driver = scope.context();
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
    });
}

#[test]
fn detach_keeps_ready() {
    Runtime::throughput().run(|scope| {
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
    });
}
