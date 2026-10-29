use std::{convert::Infallible, pin::pin};

use common::runtime::Setup;
use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    context::Context,
    wait::{Capacity, Queue, Registration, Waiter},
};
use dope_test::alloc::Tracker;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn wakes_after_cancellation() {
    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let keys = [0_u16, 1].map(|index| {
                site.targets::<KeyTag<0>>()
                    .bind(SlotIndex::from(index), Epoch::INITIAL)
                    .dispatch()
            });
            let [first, second] = keys.map(|key| site.ready().make_slot(key).expect("ready slot"));
            let queue = Queue::new(site, Capacity::new(3));
            let left = pin!(Waiter::new());
            let right = pin!(Waiter::new());
            for _ in 0..2 {
                for (target, waiter) in [
                    (first.target(), left.as_ref()),
                    (second.target(), right.as_ref()),
                ] {
                    Context::with_target(target, turn.turn().app(), driver.reborrow(), |cx| {
                        assert!(queue.try_register(waiter, cx.as_ref()).is_ok());
                    });
                }
                {
                    let canceled = pin!(Waiter::new());
                    Context::with_target(
                        first.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |cx| {
                            assert!(queue.try_register(canceled.as_ref(), cx.as_ref()).is_ok());
                        },
                    );
                }
                assert_eq!(queue.len().get(), 2);
                let ((), usage) = Tracker::usage(|| queue.wake_all());
                assert_eq!(usage, (0, 0));
                assert_eq!(queue.len().get(), 0);
                assert!(matches!(left.registration(), Registration::Vacant));
                assert!(matches!(right.registration(), Registration::Vacant));
                assert_eq!(
                    turn.ready()
                        .drain(|token| assert!(keys.iter().any(|key| key.matches(token))))
                        .map_or(0, |count| count.get()),
                    2
                );
            }
        },
        recovery::Policy::new(
            |event| {
                assert!(matches!(event, dope::core::io::Reclaimed::Shutdown));
            },
            recover,
        ),
        |result| result.expect("driver lifecycle"),
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
