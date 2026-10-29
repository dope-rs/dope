use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
use o3::depot::queue::resident;

use crate::common::wake::Ready as _;
pub mod common;

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::{
        wake,
        wake::{Drain, fanout, handoff, queue, table},
    },
};

use crate::common::{
    rt::Runtime,
    wake::{MANY_LANES, ONE_LANE},
};

#[test]
fn waiters_respect_budget() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let target = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = (driver.ready().make_slot(target)).expect("ready slot");
            let fanout = (fanout::Set::try_new(driver, 1)).expect("fanout registry");
            let mut wait = fanout::Registration::new();
            assert!(matches!(
                fanout.try_register(&mut wait, &wake::Waker::from(ready.target())),
                Ok(())
            ));
            fanout.publish();

            let work = turn.turn().app();
            while work.permit().is_some() {}
            assert!((fanout.drain_one(work)) == (Drain::Exhausted));

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

#[test]
fn exhaustion_keeps_owner() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let q_target = targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch();
            let x_target = targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch();
            let q_slot = (driver.ready().make_slot(q_target)).expect("queued ready slot");
            let x_slot = (driver.ready().make_slot(x_target)).expect("exact ready slot");
            let queued = (queue::Queue::try_new(driver, 1, ONE_LANE)).expect("queue registry");
            let exact = (table::Table::try_new(driver, 1)).expect("exact registry");
            let mut q_wait = queue::Registration::new();
            let mut x_wait = table::Registration::new();

            assert!(matches!(
                queued.try_register(0, &mut q_wait, &wake::Waker::from(q_slot.target())),
                Ok(())
            ));
            assert!(matches!(
                exact.try_register(0, &mut x_wait, &wake::Waker::from(x_slot.target())),
                Ok(())
            ));
            queued.begin_drain();
            exact.begin_drain();

            let work = turn.turn().app();
            while work.permit().is_some() {}
            assert!((queued.drain_one(work)) == (Drain::Exhausted));
            assert!((exact.drain_one(work)) == (Drain::Exhausted));
            assert!(queued.has_waiters());
            assert!(exact.has_waiters());
            assert!(q_wait.is_active());

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
#[test]
fn empty_costs_no_budget() {
    let mut reports = Vec::new();

    Runtime::throughput().with_turn(
        |turn, driver| {
            let driver = driver.driver_ref();
            let registry = (queue::Queue::try_new(driver, 0, MANY_LANES)).expect("queue registry");
            let exact = (table::Table::try_new(driver, 4_096)).expect("exact table registry");
            let handoff = (handoff::Queue::<(), _>::try_new(driver, 0, resident::Fixed::<4_096>))
                .expect("handoff registry");
            let fanout = (fanout::Set::try_new(driver, 0)).expect("fanout set");
            let work = turn.turn().app();
            while work.permit().is_some() {}

            registry.begin_drain();
            assert!((registry.drain_one(work)) == (Drain::Empty));
            assert!(!registry.is_draining());
            exact.begin_drain();
            assert!((exact.drain_one(work)) == (Drain::Empty));
            assert!(!exact.is_draining());
            handoff.begin_drain();
            assert!((handoff.drain_one(work)) == (Drain::Empty));
            fanout.publish();
            assert!((fanout.drain_one(work)) == (Drain::Empty));
            assert!(!fanout.has_pending());
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
