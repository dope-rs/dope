use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
use o3::depot::queue::resident::{
    self,
    handoff::{Cancel, Grant, Reject},
};

use crate::common::wake::Ready as _;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::{
        wake,
        wake::{Drain, handoff},
    },
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn fifo_cancel_and_alloc() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let target_a = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let target_b = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let target_c = targets
                .bind(SlotIndex::from(2_u16), Epoch::INITIAL)
                .dispatch();
            let slot_a = (driver.ready().make_slot(target_a)).expect("first ready slot");
            let slot_b = (driver.ready().make_slot(target_b)).expect("second ready slot");
            let slot_c = (driver.ready().make_slot(target_c)).expect("third ready slot");
            let registry = (handoff::Queue::<u32, _>::try_new(driver, 3, resident::Fixed::<1>))
                .expect("handoff registry");
            let mut first = handoff::Registration::new();
            let mut second = handoff::Registration::new();
            let mut third = handoff::Registration::new();

            let ((), alloc) = Tracker::usage(|| {
                assert!(matches!(
                    registry.try_register::<0>(&mut first, &wake::Waker::from(slot_a.target())),
                    Ok(())
                ));
                assert!(matches!(
                    registry.try_register::<0>(&mut second, &wake::Waker::from(slot_b.target())),
                    Ok(())
                ));
                assert!(matches!(
                    registry.try_register::<0>(&mut third, &wake::Waker::from(slot_c.target())),
                    Ok(())
                ));
                assert!(matches!(second.cancel(), Cancel::Wake));

                assert!(matches!(registry.offer::<0>(41), handoff::Offer::Woke));
                assert!((first.take_grant()) == (Grant::Ready(41)));
                assert!(matches!(first.cancel(), Cancel::Empty));

                assert!(matches!(registry.offer::<0>(42), handoff::Offer::Woke));
                assert!((third.take_grant()) == (Grant::Ready(42)));
                assert!(matches!(third.cancel(), Cancel::Empty));
            });
            assert_eq!(alloc, (0, 0));
            assert!(registry.is_empty::<0>());

            let mut wakes = Vec::with_capacity(2);
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 2);
            assert!(target_a.matches(wakes[0]));
            assert!(target_c.matches(wakes[1]));
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
            let next = targets
                .bind(SlotIndex::from(2_u16), Epoch::INITIAL)
                .dispatch();
            let old_slot = (driver.ready().make_slot(old)).expect("original ready slot");
            let busy = (driver.ready().make_slot(full)).expect("occupied ready slot");
            let new_slot = (driver.ready().make_slot(next)).expect("replacement ready slot");
            let original = (handoff::Queue::<u32, _>::try_new(driver, 1, resident::Fixed::<1>))
                .expect("original handoff registry");
            let dst = (handoff::Queue::<u32, _>::try_new(driver, 1, resident::Fixed::<1>))
                .expect("dst handoff registry");
            let mut wait = handoff::Registration::new();
            let mut occupied = handoff::Registration::new();

            assert!(matches!(
                original.try_register::<0>(&mut wait, &wake::Waker::from(old_slot.target())),
                Ok(())
            ));
            assert!(matches!(
                dst.try_register::<0>(&mut occupied, &wake::Waker::from(busy.target())),
                Ok(())
            ));
            assert!(matches!(
                dst.try_register::<0>(&mut wait, &wake::Waker::from(new_slot.target())),
                Err(Reject::Active { .. })
            ));
            assert!(matches!(original.offer::<0>(71), handoff::Offer::Woke));
            assert!((wait.take_grant()) == (Grant::Ready(71)));
            assert!(matches!(wait.cancel(), Cancel::Empty));

            assert!(matches!(occupied.cancel(), Cancel::Wake));
            assert!(matches!(
                dst.try_register::<0>(&mut wait, &wake::Waker::from(new_slot.target())),
                Ok(())
            ));
            assert!(matches!(dst.offer::<0>(72), handoff::Offer::Woke));
            assert!(matches!(wait.cancel(), Cancel::Grant(72)));

            let mut wakes = Vec::with_capacity(2);
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 2,);
            assert!(old.matches(wakes[0]));
            assert!(next.matches(wakes[1]));
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
fn drain_keeps_grants() {
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
            let registry = (handoff::Queue::<u32, _>::try_new(driver, 2, resident::Fixed::<1>))
                .expect("handoff registry");
            let mut first = handoff::Registration::new();
            let mut second = handoff::Registration::new();

            assert!(matches!(
                registry.try_register::<0>(&mut first, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register::<0>(&mut second, &wake::Waker::from(slot_b.target())),
                Ok(())
            ));
            assert!(matches!(registry.offer::<0>(73), handoff::Offer::Woke));
            registry.begin_drain();
            assert!(matches!(
                registry.offer::<0>(74),
                handoff::Offer::Draining(74)
            ));
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Woke));
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Empty));
            assert!(matches!(first.cancel(), Cancel::Grant(73)));
            assert!(matches!(second.cancel(), Cancel::Empty));

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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
