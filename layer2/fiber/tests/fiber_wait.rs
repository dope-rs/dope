use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::alloc;
pub mod common;

use std::{convert::Infallible, pin::pin};

use dope::core::driver::route::{Epoch, KeyTag, indices::SlotIndex};
use dope_fiber::{
    context::Context,
    wait::{Capacity, Error, Queue, Registration, Slot, Waiter, WakeStatus},
};
use dope_test::alloc::Tracker;

use crate::common::runtime::{self, Setup};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn moves_waiter() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let targets = site.targets::<KeyTag<0>>();
            let start = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let taken = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let next = targets
                .bind(SlotIndex::from(2_u16), Epoch::INITIAL)
                .dispatch();
            let origin = (site.ready().make_slot(start)).expect("original ready slot");
            let held = (site.ready().make_slot(taken)).expect("occupied ready slot");
            let moved = (site.ready().make_slot(next)).expect("moved ready slot");
            let queue = Queue::new(site, Capacity::new(1));
            let slot = Slot::new();
            let waiter = pin!(Waiter::new());
            let occupant = pin!(Waiter::new());

            Context::with_target(
                origin.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| {
                    ((queue.try_register(waiter.as_ref(), context.as_ref()))
                        .ok()
                        .expect("register waiter"));
                },
            );
            Context::with_target(
                held.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| {
                    ((slot.try_register(occupant.as_ref(), context.as_ref()))
                        .ok()
                        .expect("register waiter"));
                },
            );
            Context::with_target(
                moved.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| {
                    assert!(matches!(
                        slot.try_register(waiter.as_ref(), context.as_ref()),
                        Err(Error::SlotFull(_))
                    ));
                },
            );
            assert_eq!(queue.len().get(), 1);

            occupant.as_ref().unregister();
            assert!(matches!(occupant.registration(), Registration::Vacant));
            Context::with_target(
                moved.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| {
                    ((slot.try_register(waiter.as_ref(), context.as_ref()))
                        .ok()
                        .expect("register waiter"));
                },
            );
            assert_eq!(queue.len().get(), 0);
            Context::with_target(
                origin.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| {
                    ((queue.try_register(waiter.as_ref(), context.as_ref()))
                        .ok()
                        .expect("register waiter"));
                },
            );
            slot.wake();
            assert!(matches!(waiter.registration(), Registration::Active));
            assert_eq!(queue.len().get(), 1);
            assert!(matches!(
                queue.wake_one(turn.turn().app()),
                WakeStatus::Complete
            ));
            assert!(matches!(waiter.registration(), Registration::Vacant));

            let mut woken = Vec::new();
            assert_eq!(
                turn.ready()
                    .drain(|token| woken.push(token))
                    .map_or(0, |count| count.get()),
                2,
            );
            assert!(start.matches(woken[0]));
            assert!(next.matches(woken[1]));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn allocates_nothing() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(target)).expect("ready slot");
            let (queue, build) = Tracker::usage(|| Queue::new(site, Capacity::new(1)));
            assert_eq!(build, (0, 0));
            let waiter = pin!(Waiter::new());
            let (status, hot) = Tracker::usage(|| {
                assert!(matches!(
                    Context::with_target(
                        ready.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |context| queue.try_register(waiter.as_ref(), context.as_ref()),
                    ),
                    Ok(())
                ));
                queue.wake_one(turn.turn().app())
            });
            assert!(matches!(status, WakeStatus::Complete));
            assert_eq!(hot, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn keeps_replacement_order() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let keys = [0_u16, 1, 2].map(|index| {
                site.targets::<KeyTag<0>>()
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch()
            });
            let [first, second, next] =
                keys.map(|key| site.ready().make_slot(key).expect("ready slot"));
            let queue = Queue::new(site, Capacity::new(2));
            let left = pin!(Waiter::new());
            let right = pin!(Waiter::new());
            let ((), hot) = Tracker::usage(|| {
                for (target, waiter) in [
                    (first.target(), left.as_ref()),
                    (second.target(), right.as_ref()),
                ] {
                    Context::with_target(target, turn.turn().app(), driver.reborrow(), |context| {
                        ((queue.try_register(waiter, context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                    });
                }
                Context::with_target(
                    next.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        ((queue.try_register(left.as_ref(), context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                    },
                );
                assert_eq!(queue.len().get(), 2);
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(keys[0].matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
                assert!(matches!(
                    queue.wake_one(turn.turn().app()),
                    WakeStatus::Complete
                ));
                assert!(matches!(left.registration(), Registration::Vacant));
                assert!(matches!(right.registration(), Registration::Active));
                assert_eq!(queue.len().get(), 1);
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(keys[2].matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
                assert!(matches!(
                    queue.wake(turn.turn().app()),
                    WakeStatus::Complete
                ));
                assert!(matches!(right.registration(), Registration::Vacant));
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(keys[1].matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
            });
            assert_eq!(hot, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn preserves_full_source() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let key = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let rejected = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(key)).expect("ready slot");
            let returned = (site.ready().make_slot(rejected)).expect("rejected ready slot");
            let source = Queue::new(site, Capacity::new(1));
            let full = Queue::new(site, Capacity::new(0));
            let waiter = pin!(Waiter::new());
            let ((), hot) = Tracker::usage(|| {
                Context::with_target(
                    ready.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        ((source.try_register(waiter.as_ref(), context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                    },
                );
                Context::with_target(
                    returned.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        let (capacity, wake) = {
                            let result = full.try_register(waiter.as_ref(), context.as_ref());
                            assert!(
                                matches!(result, Err(Error::QueueFull { .. })),
                                "exact queue failure"
                            );
                            match result {
                                Err(Error::QueueFull { capacity, wake }) => (capacity, wake),
                                _ => return,
                            }
                        };
                        assert!(capacity == Capacity::new(0));
                        assert_eq!(source.len().get(), 1);
                        assert_eq!(full.len().get(), 0);
                        assert!(matches!(waiter.registration(), Registration::Active));
                        wake.wake();
                    },
                );
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(rejected.matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
                assert!(matches!(waiter.registration(), Registration::Active));
                assert!(matches!(
                    source.wake_one(turn.turn().app()),
                    WakeStatus::Complete
                ));
            });
            assert_eq!(hot, (0, 0));
            assert!(matches!(waiter.registration(), Registration::Vacant));
            assert_eq!(
                turn.ready()
                    .drain(|token| assert!(key.matches(token)))
                    .map_or(0, |count| count.get()),
                1
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn unlinks_on_drop() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let key = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(key)).expect("ready slot");
            let queue = Queue::new(site, Capacity::new(1));
            let slot = Slot::new();
            let ((), hot) = Tracker::usage(|| {
                Context::with_target(
                    ready.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        {
                            let waiter = pin!(Waiter::new());
                            ((queue.try_register(waiter.as_ref(), context.as_ref()))
                                .ok()
                                .expect("register waiter"));
                        }
                        assert_eq!(queue.len().get(), 0);
                        {
                            let waiter = pin!(Waiter::new());
                            ((slot.try_register(waiter.as_ref(), context.as_ref()))
                                .ok()
                                .expect("register waiter"));
                        }
                        slot.wake();
                        let waiter = pin!(Waiter::new());
                        ((slot.try_register(waiter.as_ref(), context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                        waiter.as_ref().unregister();
                        assert!(matches!(waiter.registration(), Registration::Vacant));
                        ((queue.try_register(waiter.as_ref(), context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                        waiter.as_ref().unregister();
                        assert_eq!(queue.len().get(), 0);
                    },
                );
            });
            assert_eq!(hot, (0, 0));
            assert!(turn.ready().drain(|_| {}).is_none());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn keeps_exhausted_waiter() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let key = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (site.ready().make_slot(key)).expect("ready slot");
            let queue = Queue::new(site, Capacity::new(1));
            let slot = Slot::new();
            let waiter = pin!(Waiter::new());
            let work = turn.turn().app();
            Context::with_target(ready.target(), work, driver.reborrow(), |context| {
                ((queue.try_register(waiter.as_ref(), context.as_ref()))
                    .ok()
                    .expect("register waiter"));
            });
            for _ in 0..work.remaining() {
                assert!(work.permit().is_some());
            }
            assert!(matches!(queue.wake_one(work), WakeStatus::Pending));
            assert!(matches!(queue.wake(work), WakeStatus::Pending));
            assert_eq!(queue.len().get(), 1);
            assert!(matches!(waiter.registration(), Registration::Active));
            waiter.as_ref().unregister();
            assert!(matches!(queue.wake_one(work), WakeStatus::Complete));
            assert!(matches!(queue.wake(work), WakeStatus::Complete));
            Context::with_target(ready.target(), work, driver.reborrow(), |context| {
                slot.try_register(waiter.as_ref(), context.as_ref())
                    .ok()
                    .expect("register slot waiter");
            });
            assert!(matches!(slot.wake_one(work), WakeStatus::Pending));
            waiter.as_ref().unregister();
            assert!(matches!(slot.wake_one(work), WakeStatus::Complete));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn replaces_occupied_slot() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let targets = site.targets::<KeyTag<0>>();
            let old = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let new = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let previous = (site.ready().make_slot(old)).expect("previous ready slot");
            let next = (site.ready().make_slot(new)).expect("next ready slot");
            let slot = Slot::new();
            let waiter = pin!(Waiter::new());
            let ((), hot) = Tracker::usage(|| {
                Context::with_target(
                    previous.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        for _ in 0..2 {
                            slot.try_register(waiter.as_ref(), context.as_ref())
                                .ok()
                                .expect("register waiter");
                        }
                    },
                );
                assert!(turn.ready().drain(|_| {}).is_none());
                Context::with_target(
                    next.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        ((slot.try_register(waiter.as_ref(), context.as_ref()))
                            .ok()
                            .expect("register waiter"));
                    },
                );
                assert!(matches!(waiter.registration(), Registration::Active));
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(old.matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
                slot.wake();
                assert!(matches!(waiter.registration(), Registration::Vacant));
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(new.matches(token)))
                        .map_or(0, |count| count.get()),
                    1
                );
                slot.wake();
                assert!(turn.ready().drain(|_| {}).is_none());
            });
            assert_eq!(hot, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn check_reports(reports: &[Reclaimed]) {
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
