use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule,
    schedule::ready::{wake, wake::store},
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn reuses_ready_pool() {
    let mut reports = Vec::new();

    Runtime::throughput().with_scope(
        |scope| {
            scope.with_driver(|ctx, mut control| {
                let driver = ctx.driver_ref();
                let target = driver
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch();

                let ((), usage) = Tracker::usage(|| {
                    for cycle in 0..4 {
                        {
                            let turn = control.begin();
                            for step in 0..schedule::LIMIT {
                                let slot = (driver.ready().make_slot(target))
                                    .expect("recycled dynamic ready slot");
                                slot.activate();
                                let mut rounds = turn.ready();
                                let index = cycle * schedule::LIMIT + step;
                                assert!(
                                    rounds
                                        .drain(|token| assert!(target.matches(token)))
                                        .is_some(),
                                    "ready slot was not drained at iteration {index}",
                                );
                            }
                        }
                    }
                });
                assert_eq!(usage, (0, 0));
            });
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
fn wakes_after_vacancy() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, ctx| {
            let driver = ctx.driver_ref();
            let target = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (driver.ready().make_slot(target))
                .expect("dynamic ready slot must remain available");
            let ((), usage) = Tracker::usage(|| {
                let wakes = store::Store::new(driver);
                let entry = store::Entry::empty();
                wakes.set(&entry, wake::Waker::from(ready.target()));
                wakes.wake_once(&entry);
                wakes.wake_once(&entry);
            });
            assert_eq!(usage, (0, 0));
            let mut rounds = turn.ready();
            assert!(
                rounds
                    .drain(|token| assert!(target.matches(token)))
                    .is_some()
            );
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
fn ignores_retired_wake() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, ctx| {
            let driver = ctx.driver_ref();
            let target = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let stale = {
                let first = (driver.ready().make_slot(target))
                    .expect("first ready slot must remain available");
                first.target().key()
            };
            let next =
                (driver.ready().make_slot(target)).expect("released ready slot must be reusable");
            let ((), usage) = Tracker::usage(|| {
                stale.wake(driver);
                assert!(
                    turn.ready()
                        .drain(|token| assert!(!target.matches(token), "retired slot woke"))
                        .is_none()
                );
                next.target().key().wake(driver);
                assert!(
                    turn.ready()
                        .drain(|token| assert!(target.matches(token)))
                        .is_some()
                );
            });
            assert_eq!(usage, (0, 0));
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
