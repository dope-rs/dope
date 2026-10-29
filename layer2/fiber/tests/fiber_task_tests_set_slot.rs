use std::{convert::Infallible, pin::pin};

use dope::core::driver::{
    life::recovery::{self, Policy},
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    abi::Ready,
    context::wake::RootWaker,
    task::storage::{
        scope::Scope,
        set::{Access as _, Set},
    },
};
use o3::depot::capacity::Count;

use crate::common::runtime::{self, Setup};
pub mod common;

use crate::common::schedule::tasks::fixtures::*;

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

#[test]
fn preserves_target_type() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let ready = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(ready.target());
            let target = WideTarget(u128::MAX - 17);
            let tasks: Set<'_, Ready<()>, WideTarget, Domain> =
                Set::try_new(Count::new(1)).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                assert!(tasks.try_insert(Ready::new(()), target, parent).is_ok());
                assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 1);
                let (actual, ()) = tasks
                    .take_done()
                    .expect("completed task must retain its output");
                assert!(actual == target);
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
