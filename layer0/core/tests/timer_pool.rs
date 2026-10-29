use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::wake::Waker,
    settings,
};
use dope_test::alloc::Tracker;
use settings::demands::slots;

use crate::common::rt::Runtime;

#[test]
fn reuses_slot_no_alloc() {
    let mut reports = Vec::new();

    Runtime::throughput()
        .timers(slots::Slots::fixed::<1>())
        .with_turn(
            |turn, driver| {
                let core = driver.driver_ref();
                let target = core
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch();
                let ready = (core.ready().make_slot(target)).expect("timer ready slot");
                let timer = driver.driver_ref().timer();
                let now = turn.turn().clock().now();
                let deadline = now.into();

                let ((), alloc) = Tracker::usage(|| {
                    for step in 0..1_024 {
                        {
                            let alarm = (timer.try_alarm()).expect("timer slot");
                            alarm.arm(deadline, Waker::from(ready.target()));
                            assert!(alarm.is_armed(), "alarm {step}");
                            alarm.clear();
                            assert!(!alarm.is_armed(), "alarm {step}");
                        }
                        turn.expire();
                    }
                });
                assert_eq!(alloc, (0, 0));
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
