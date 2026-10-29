use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
use o3::depot::queue::resident;

use crate::common::wake::Ready as _;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::{
        wake,
        wake::{Drain, queue},
    },
};
use dope_test::alloc::Tracker;

use crate::common::{
    rt::Runtime,
    wake::{FOUR_LANES, ONE_LANE, THREE_LANES, TWO_LANES},
};

#[test]
fn scoped_index_wakes() {
    let mut reports = Vec::new();
    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let target = driver.targets::<KeyTag<0>>()
                .bind(SlotIndex::from(0_u16), Epoch::INITIAL)
                .dispatch();
            let slot = driver.ready().make_slot(target).expect("ready slot");
            queue::Queue::try_with(driver, 1, TWO_LANES, (), |_, queue| {
                let index = queue.try_index(1).expect("second lane");
                assert!(matches!(queue.try_index(2), Err(resident::Bounds { index: 2, len }) if len.get() == 2));
                let mut entry = queue::Registration::new();
                queue.try_register(index, &mut entry, &wake::Waker::from(slot.target()))
                    .expect("scoped registration");
                assert!(matches!(queue.wake_one(index), wake::Once::Woke));
                assert_eq!(turn.ready_round(|ready| assert!(target.matches(ready))), 1);
            }).expect("scoped queue");
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
fn lanes_keep_fifo() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let mut ready = Vec::new();
            for index in 0_u16..3 {
                let target = targets
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch();
                let slot = (driver.ready().make_slot(target)).expect("ready slot");
                ready.push((target, slot));
            }
            let registry = (queue::Queue::try_new(driver, 3, TWO_LANES)).expect("queue registry");
            let mut first = queue::Registration::new();
            let mut second = queue::Registration::new();
            let mut peer = queue::Registration::new();
            assert!(matches!(
                registry.try_register(1, &mut first, &wake::Waker::from(ready[0].1.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(1, &mut second, &wake::Waker::from(ready[1].1.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(0, &mut peer, &wake::Waker::from(ready[2].1.target())),
                Ok(())
            ));

            assert!(matches!(registry.wake_first(), wake::Once::Woke));
            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(ready[2].0.matches(wakes[0]));
            assert!(matches!(registry.wake_first(), wake::Once::Empty));

            for index in [0, 1] {
                assert!(matches!(registry.try_wake_one(1), Ok(wake::Once::Woke)));
                let mut wakes = Vec::new();
                assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
                assert!(ready[index].0.matches(wakes[0]));
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
fn next_is_round_robin() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let mut bound = Vec::new();
            for index in 0_u16..3 {
                let target = targets
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch();
                let slot = (driver.ready().make_slot(target)).expect("ready slot");
                bound.push((target, slot));
            }

            let registry = (queue::Queue::try_new(driver, 3, THREE_LANES)).expect("queue registry");
            let mut first = queue::Registration::new();
            let mut second = queue::Registration::new();
            let mut peer = queue::Registration::new();
            assert!(matches!(
                registry.try_register(2, &mut first, &wake::Waker::from(bound[0].1.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(2, &mut second, &wake::Waker::from(bound[1].1.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(0, &mut peer, &wake::Waker::from(bound[2].1.target())),
                Ok(())
            ));

            let mut wakes = Vec::with_capacity(3);
            let ((), alloc) = Tracker::usage(|| {
                assert!(registry.has_waiters());
                assert!(matches!(registry.wake_next(), wake::Once::Woke));
                assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
                assert!(matches!(registry.wake_next(), wake::Once::Woke));
                assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
                assert!(matches!(registry.wake_next(), wake::Once::Woke));
                assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
                assert!(matches!(registry.wake_next(), wake::Once::Empty));
                assert!(!registry.has_waiters());
            });
            assert_eq!(alloc, (0, 0));

            let mut order = Vec::new();
            for token in &wakes {
                let index = (bound.iter().position(|pair| pair.0.matches(*token)))
                    .expect("known ready target");
                order.push(index);
            }
            assert_eq!(order, [0, 2, 1]);
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
fn wake_reserves_rearm() {
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
            let registry = (queue::Queue::try_new(driver, 1, TWO_LANES)).expect("queue registry");
            let mut wait = queue::Registration::new();
            let mut overflow = queue::Registration::new();

            assert!(matches!(
                registry.try_register(0, &mut wait, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            let ((), alloc) = Tracker::usage(|| {
                assert!(matches!(
                    registry.try_register(1, &mut wait, &wake::Waker::from(slot_a.target())),
                    Ok(())
                ));
                assert!(matches!(registry.try_wake_one(0), Ok(wake::Once::Empty)));
                assert!(matches!(registry.try_wake_one(1), Ok(wake::Once::Woke)));
            });
            assert_eq!(alloc, (0, 0));
            assert!(wait.is_active());
            assert!(matches!(
                registry.try_register(0, &mut overflow, &wake::Waker::from(slot_b.target())),
                Err(queue::Error::Full { .. })
            ));

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(target_a.matches(wakes[0]));

            assert!(matches!(
                registry.try_register(1, &mut wait, &wake::Waker::from(slot_b.target())),
                Ok(())
            ));
            assert!(matches!(registry.try_wake_one(1), Ok(wake::Once::Woke)));
            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(target_b.matches(wakes[0]));

            wait.clear();
            assert!(matches!(
                registry.try_register(0, &mut overflow, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
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
fn failed_move_keeps_owner() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let target_a = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let full = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let next = targets
                .bind(SlotIndex::from(2_u16), Epoch::INITIAL)
                .dispatch();
            let slot_a = (driver.ready().make_slot(target_a)).expect("first ready slot");
            let busy = (driver.ready().make_slot(full)).expect("occupied ready slot");
            let new_slot = (driver.ready().make_slot(next)).expect("replacement ready slot");
            let original = (queue::Queue::try_new(driver, 1, ONE_LANE)).expect("original queue");
            let filled = (queue::Queue::try_new(driver, 1, ONE_LANE)).expect("filled queue");
            let mut wait = queue::Registration::new();
            let mut occupied = queue::Registration::new();
            assert!(matches!(
                original.try_register(0, &mut wait, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            assert!(matches!(
                filled.try_register(0, &mut occupied, &wake::Waker::from(busy.target())),
                Ok(())
            ));
            assert!(matches!(
                filled.try_register(0, &mut wait, &wake::Waker::from(new_slot.target())),
                Err(queue::Error::Active { .. })
            ));
            assert!(matches!(original.try_wake_one(0), Ok(wake::Once::Woke)));

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(target_a.matches(wakes[0]));
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
fn hot_path_has_no_alloc() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let target = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (driver.ready().make_slot(target)).expect("ready slot");
            let registry =
                queue::Queue::try_new(driver, 1, resident::Fixed::<1>).expect("queue registry");
            let mut wait = queue::Registration::new();

            let ((), alloc) = Tracker::usage(|| {
                registry
                    .try_register::<0>(&mut wait, &wake::Waker::from(ready.target()))
                    .expect("wake registration");
                assert!(matches!(registry.wake_one::<0>(), wake::Once::Woke));
                wait.clear();
            });
            assert_eq!(alloc, (0, 0));

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(target.matches(wakes[0]));
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
fn drain_is_resumable() {
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
            let registry = (queue::Queue::try_new(driver, 2, FOUR_LANES)).expect("queue registry");
            let mut first = queue::Registration::new();
            let mut second = queue::Registration::new();
            assert!(matches!(
                registry.try_register(3, &mut first, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(3, &mut second, &wake::Waker::from(slot_b.target())),
                Ok(())
            ));
            registry.begin_drain();

            let work = turn.turn().app();
            while work.permit().is_some() {}
            assert!((registry.drain_one(work)) == (Drain::Exhausted));
            assert!(registry.is_draining());
            assert!(first.is_active());
            assert!(second.is_active());
            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 0,);
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
