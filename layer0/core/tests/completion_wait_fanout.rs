use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;

use crate::common::wake::Ready as _;
pub mod common;

use dope_core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::{
        wake,
        wake::{Drain, fanout},
    },
};

use crate::common::rt::Runtime;

#[test]
fn reuse_is_bounded() {
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

            let registry = (fanout::Set::try_new(driver, 2)).expect("fanout registry");
            let mut first = fanout::Registration::new();
            let mut second = fanout::Registration::new();
            let mut overflow = fanout::Registration::new();

            assert!(matches!(
                registry.try_register(&mut first, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(&mut second, &wake::Waker::from(slot_b.target())),
                Ok(())
            ));
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Empty));

            registry.publish();
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Woke));
            second.clear();
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Empty));

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 1,);
            assert!(target_a.matches(wakes[0]));

            assert!(matches!(
                registry.try_register(&mut first, &wake::Waker::from(slot_a.target())),
                Ok(())
            ));
            assert!(matches!(
                registry.try_register(&mut overflow, &wake::Waker::from(slot_b.target())),
                Ok(())
            ));
            registry.publish();
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Woke));
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Woke));
            assert!((registry.drain_one(turn.turn().app())) == (Drain::Empty));

            let mut wakes = Vec::new();
            assert_eq!(turn.ready_round(|token| wakes.push(token)), 2,);
            assert!(wakes.iter().any(|token| target_a.matches(*token)));
            assert!(wakes.iter().any(|token| target_b.matches(*token)));
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
