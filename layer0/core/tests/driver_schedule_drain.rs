use std::{convert::Infallible, pin};

use dope_core::driver::{
    life::recovery,
    schedule::{
        self, app,
        ready::{
            task,
            task::{raw, topology},
        },
    },
};
use dope_test::scope::Error;

use crate::common::{
    driver::{Scoped as _, support::Targets as _, task::Index},
    rt::Runtime,
};

pub mod common;

#[test]
fn drain_carries_admission() {
    let mut reported = false;
    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let driver = scope.context().driver_ref();
            let slot = driver
                .ready()
                .make_slot(driver.target())
                .expect("root slot");
            let parent = task::Parent::from(slot.target());
            let ready = pin::pin!(topology::Slot::new());
            // SAFETY: the admitted parent and pinned topology remain live until binding drop.
            let binding = unsafe {
                raw::Root::bind(
                    parent.try_admit().expect("task slot"),
                    ready.try_attach(parent, Index(0)).expect("topology"),
                )
            };
            binding.poll().1.wake();
            scope.run_turn(|_, turn| {
                let work = turn.turn().app();
                let mut drain = ready.drain().expect("ready drain");
                let permit = match work.next(&mut drain) {
                    app::Next::Item(_, permit) => Some(permit),
                    _ => None,
                }
                .expect("admission");
                permit.run(|| assert_eq!(work.remaining(), schedule::LIMIT - 1));
                assert!(matches!(work.next(&mut drain), app::Next::Empty));
                assert_eq!(work.remaining(), schedule::LIMIT - 1);
                drop(drain);
                binding.poll().1.wake();
                while let Some(permit) = work.permit() {
                    permit.run(|| ());
                }
                let mut drain = ready.drain().expect("next drain");
                assert!(matches!(work.next(&mut drain), app::Next::Exhausted(_)));
                assert!(drain.peek().is_some());
            });
            scope.run_turn(|_, turn| {
                let mut drain = ready.drain().expect("retained candidate");
                let permit = match turn.turn().app().next(&mut drain) {
                    app::Next::Item(_, permit) => Some(permit),
                    _ => None,
                }
                .expect("retained admission");
                permit.run(|| ());
            });
        },
        recovery::Policy::new(|_| reported = true, recover),
        finish,
    );
    assert!(!reported);
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
