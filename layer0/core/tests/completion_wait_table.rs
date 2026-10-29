use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;

use crate::common::wake::{Completion, Ready as _};
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::{wake, wake::table, wake::table::raw::Index as _},
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn rearm_is_affine() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let target_a = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let target_b = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let slot_a = (driver.ready().make_slot(target_a)).expect("first ready slot");
            let slot_b = (driver.ready().make_slot(target_b)).expect("second ready slot");
            let registry = Completion::try_new(driver).expect("exact table registry");
            assert!(matches!(
                registry.table().try_wake_one(2),
                Err(bounds) if bounds.index == 2 && bounds.len == 2
            ));
            let mut wait = table::Registration::new();
            let mut occupied = table::Registration::new();

            let ((), alloc) = Tracker::usage(|| {
                registry.with(|slot, sink| {
                    assert!(matches!(
                        slot.try_register(&sink, &mut wait, &wake::Waker::from(slot_a.target())),
                        Ok(())
                    ));
                    assert!(matches!(
                        slot.try_register(
                            &sink,
                            &mut occupied,
                            &wake::Waker::from(slot_b.target())
                        ),
                        Err(table::Error::Occupied { .. })
                    ));
                    assert!(matches!(slot.wake(&sink), wake::Once::Woke));
                    assert!(matches!(
                        slot.try_register(&sink, &mut wait, &wake::Waker::from(slot_b.target())),
                        Ok(())
                    ));
                });
                let work = turn.turn().app();
                let left = work.remaining();
                assert!(registry.table().drain_one(work) == wake::Drain::Empty);
                assert_eq!(work.remaining(), left);
                registry.table().begin_drain();
                registry.with(|slot, sink| {
                    assert!(matches!(
                        slot.try_register(&sink, &mut wait, &wake::Waker::from(slot_b.target())),
                        Err(table::Error::Draining { .. })
                    ));
                });
                assert!(registry.table().drain_one(work) == wake::Drain::Woke);
                assert_eq!(work.remaining(), left - 1);
                assert!(registry.table().drain_one(work) == wake::Drain::Empty);
                assert_eq!(work.remaining(), left - 1);
                assert!(!registry.table().is_draining());
                wait.clear();
                registry.with(|slot, sink| {
                    assert!(matches!(
                        slot.try_register(
                            &sink,
                            &mut occupied,
                            &wake::Waker::from(slot_a.target())
                        ),
                        Ok(())
                    ));
                });
                occupied.clear();
            });
            assert_eq!(alloc, (0, 0));

            let mut wakes = Vec::with_capacity(2);
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 2,);
            assert!(target_a.matches(wakes[0]));
            assert!(target_b.matches(wakes[1]));
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
fn foreign_keeps_owner() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let old = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let full = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let old_slot = (driver.ready().make_slot(old)).expect("original ready slot");
            let busy = (driver.ready().make_slot(full)).expect("occupied ready slot");
            let source = (table::Table::try_new(driver, 1)).expect("source table");
            let dst = Completion::try_new(driver).expect("exact table");
            let mut wait = table::Registration::new();
            let mut occupied = table::Registration::new();

            assert!(matches!(
                source.try_register(0, &mut wait, &wake::Waker::from(old_slot.target())),
                Ok(())
            ));
            dst.with(|slot, sink| {
                assert!(matches!(
                    slot.try_register(&sink, &mut occupied, &wake::Waker::from(busy.target())),
                    Ok(())
                ));
                assert!(matches!(
                    slot.try_register(&sink, &mut wait, &wake::Waker::from(busy.target())),
                    Err(table::Error::Active { .. })
                ));
            });
            assert!(
                matches!(source.try_wake_one(0), Ok(wake::Once::Woke)),
                "failed move must retain the source",
            );
            wait.clear();

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(old.matches(wakes[0]));
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
