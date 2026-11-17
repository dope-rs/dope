use std::{pin::pin, task::Poll, time::Duration};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{self, clock::Sample},
    settings::demands::slots::Slots,
};
use dope_fiber::{
    abi::{Task, poll_fn},
    context::Context,
    task::time::sleep::Sleep,
};

pub mod common;
use common::recover;
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
        runtime::check_scope,
    );
}

#[test]
fn until_preserves_origin() {
    Setup::throughput().timers(Slots::fixed::<2>()).with_scope(
        |scope| {
            let (_, mut driver, mut schedule) = scope.split();
            let core = driver.driver_ref();
            let start = core
                .clock()
                .now()
                .try_add(Duration::from_secs(86_400))
                .expect("manual origin");
            let deadline = start.try_add(Duration::from_millis(19)).expect("deadline");
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let mut until = Sleep::until(core.timer(), deadline).expect("absolute timer");
            let mut relative =
                Sleep::new(core.timer(), Duration::from_millis(10)).expect("relative timer");
            for (millis, expected) in [
                (0, (Poll::Pending, Poll::Pending)),
                (10, (Poll::Pending, Poll::Ready(()))),
                (19, (Poll::Ready(()), Poll::Pending)),
            ] {
                let now = start
                    .try_add(Duration::from_millis(millis))
                    .expect("turn time");
                let turn = schedule.begin_at(Sample::new(now.into_raw()));
                let mut fiber = pin!(Task::new(poll_fn(|mut context| {
                    Poll::Ready((
                        until.poll_sleep(context.as_mut()),
                        relative.poll_sleep(context),
                    ))
                })));
                turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                    Context::with_target(
                        root.target(),
                        share.work(),
                        driver.reborrow(),
                        |context| {
                            assert_eq!(context.poll(fiber.as_mut()), Poll::Ready(expected));
                        },
                    );
                });
            }
        },
        recovery::Policy::new(|_| {}, recover),
        runtime::check_scope,
    );
}
