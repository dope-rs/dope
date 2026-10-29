use std::{convert::Infallible, pin::pin, task::Poll, time::Duration};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule,
    settings::demands::slots::Slots,
};
use dope_fiber::{
    abi::{Task, poll_fn},
    context::Context,
    task::time::sleep::Sleep,
};

pub mod common;
use common::runtime::{self, Setup};

#[test]
fn sleep_admission() {
    Setup::throughput().timers(Slots::fixed::<1>()).with_turn(
        |turn, driver| {
            let core = driver.driver_ref();
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let mut sleep = Sleep::new(core.timer(), Duration::ZERO).expect("timer slot");
            let mut fiber = pin!(Task::new(poll_fn(|mut context| {
                let ready = sleep.poll_sleep(context.as_mut());
                assert_eq!(sleep.poll_sleep(context), Poll::Pending);
                ready
            })));
            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                Context::with_target(root.target(), share.work(), driver, |context| {
                    assert_eq!(context.poll(fiber.as_mut()), Poll::Ready(()));
                });
            });
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
