use dope::core::driver::life::recovery;
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;

use crate::common::runtime::profile::Tasks;
pub mod common;
use common::recover;

use std::{cell::Cell, mem, pin, rc::Rc};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{self, app::step::Flow, ready::Readiness},
};
use dope_fiber::{
    abi,
    task::storage::fixed::{
        local::Removal,
        schedule::{
            Pool,
            result::{Outcome, Selection},
        },
    },
};
use dope_test::alloc::Tracker;
use o3::depot::capacity::Count;

use crate::common::{
    runtime::{self, Setup},
    schedule::tasks::fixtures::CaptureReady,
    storage::policies::Rearm,
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn drop_requeues_task() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let ready = ready.expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<()>, (), u8>::try_new(Count::new(1));
            let pool = pool.expect("task pool allocation failed");
            let mut plane = pin::pin!(pool);
            Scope::with(plane.as_mut(), |mut plane| {
                let task = plane
                    .try_admit((), parent)
                    .expect("task insertion failed")
                    .insert((Ready::new(()), 7));

                let alloc = Tracker::during(|| {
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => admitted.target() == 7,
                        Selection::Idle | Selection::Yielded => false,
                    });
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => {
                            assert_eq!(admitted.target(), 7);
                            matches!(
                                admitted.drive(&mut driver),
                                Outcome::Complete {
                                    target: 7,
                                    output: (),
                                },
                            )
                        }
                        Selection::Idle | Selection::Yielded => false,
                    });
                    assert!(matches!(plane.remove(task), Removal::Absent));
                });

                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn forgotten_keeps_order() {
    let mut reports = Vec::new();
    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(ready.target());
            let pool =
                Pool::<'_, abi::Ready<()>, (), u8>::try_new(Count::new(3)).expect("task pool");
            let mut plane = pin::pin!(pool);
            Scope::with(plane.as_mut(), |mut plane| {
                let alloc = Tracker::during(|| {
                    let [first, second, _third] = [1, 2, 3].map(|target| {
                        plane
                            .try_admit((), parent)
                            .expect("task admission")
                            .insert((Ready::new(()), target))
                    });
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(selected) => {
                            assert_eq!(selected.target(), 1);
                            mem::forget(selected);
                            true
                        }
                        Selection::Idle | Selection::Yielded => false,
                    });
                    assert!(matches!(plane.remove(second), Removal::Removed));
                    let _reused = plane
                        .try_admit((), parent)
                        .expect("replacement admission")
                        .insert((Ready::new(()), 4));
                    assert!(matches!(plane.remove(first), Removal::Removed));
                    for expected in [3, 4] {
                        assert!(match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(selected) => {
                                assert_eq!(selected.target(), expected);
                                matches!(selected.drive(&mut driver), Outcome::Complete { .. })
                            }
                            Selection::Idle | Selection::Yielded => false,
                        });
                    }
                });
                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn yield_preserves_ready() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let ready = (ready).expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<()>, (), u8>::try_new(Count::new(1));
            let pool = (pool).expect("task pool allocation failed");
            let mut plane = pin::pin!(pool);
            Scope::with(plane.as_mut(), |mut plane| {
                let task = plane
                    .try_admit((), parent)
                    .expect("task insertion failed")
                    .insert((Ready::new(()), 9));
                while turn.turn().app().permit().is_some() {}

                assert!(matches!(
                    plane.as_mut().select(turn.turn().app()),
                    Selection::Yielded
                ));
                assert!(matches!(plane.remove(task), Removal::Removed));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn external_wake_survives_requeue() {
    let mut reports = Vec::new();
    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let root = view
                .ready()
                .make_slot(
                    view.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let parent = RootWaker::from(root.target());
            let polls = Cell::new(0);
            let wake = Cell::new(None);
            let mut plane = pin::pin!(
                Pool::<'_, Rearm<'_, '_>, (), u8>::try_new(Count::new(2)).expect("task pool")
            );
            Scope::with(plane.as_mut(), |mut plane| {
                let pending = plane
                    .try_admit((), parent)
                    .expect("pending admission")
                    .insert((Rearm(Some((&polls, &wake))), 0));
                let rearming = plane
                    .try_admit((), parent)
                    .expect("rearming admission")
                    .insert((Rearm(None), 1));

                let allocated = Tracker::during(|| {
                    for expected in [0, 1, 0, 1, 0] {
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            let Selection::Ready(selected) = plane.as_mut().select(share.work())
                            else {
                                panic!("ready task");
                            };
                            let outcome = selected.drive(&mut driver);
                            assert!(matches!(
                                (expected, outcome),
                                (0, Outcome::Pending { target: 0 })
                                    | (1, Outcome::PendingReady { target: 1 })
                            ));
                            assert_eq!(share.work().remaining(), 0);
                        });
                        if expected == 0 {
                            wake.take().expect("captured wake").wake();
                        }
                    }
                });
                assert_eq!(allocated, (0, 0));
                assert_eq!(polls.get(), 3);
                assert!(matches!(plane.remove(pending), Removal::Removed));
                assert!(matches!(plane.remove(rearming), Removal::Removed));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn external_wake_survives_admissions() {
    let mut reports = Vec::new();
    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let root = view
                .ready()
                .make_slot(
                    view.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let parent = RootWaker::from(root.target());
            let pool =
                Pool::<'_, CaptureReady<'_>, (), u8>::try_new(Count::new(2)).expect("task pool");
            let mut plane = pin::pin!(pool);
            let polls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let allowed = Rc::new(Cell::new(false));
            let wake = Rc::new(Cell::new(None));
            let other_wake = Rc::new(Cell::new(None));
            Scope::with(plane.as_mut(), |mut plane| {
                let mut ids = [0, 1].map(|target| {
                    plane
                        .try_admit((), parent)
                        .expect("task admission")
                        .insert((
                            CaptureReady {
                                wake: if target == 0 {
                                    wake.clone()
                                } else {
                                    other_wake.clone()
                                },
                                drops: drops.clone(),
                                polls: polls.clone(),
                                ready: allowed.clone(),
                                output: target,
                            },
                            target,
                        ))
                });
                let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app()) else {
                    panic!("pending task");
                };
                let (outcome, receipt) = selected.step(&mut driver);
                assert!(matches!(outcome, Outcome::Pending { target: 0 }));
                receipt.settle(Flow::Batch);
                allowed.set(true);
                wake.take().expect("captured wake").wake();
                assert!(root.take() == Readiness::Ready);

                let allocated = Tracker::during(|| {
                    for (next, expected) in [1, 0, 2, 3].into_iter().enumerate() {
                        let Selection::Ready(selected) = plane.as_mut().select(turn.turn().app())
                        else {
                            panic!("ready task");
                        };
                        let (outcome, receipt) = selected.step(&mut driver);
                        assert!(matches!(
                            outcome,
                            Outcome::Complete { target, output }
                                if target == expected && output == expected
                        ));
                        receipt.settle(Flow::Batch);
                        if next == 0 {
                            assert!(root.take() == Readiness::Ready);
                        }
                        let target = next as u8 + 2;
                        let replacement = plane
                            .try_admit((), parent)
                            .expect("replacement admission")
                            .insert((
                                CaptureReady {
                                    wake: other_wake.clone(),
                                    drops: drops.clone(),
                                    polls: polls.clone(),
                                    ready: allowed.clone(),
                                    output: target,
                                },
                                target,
                            ));
                        let completed = mem::replace(&mut ids[(next + 1) % 2], replacement);
                        assert!(matches!(plane.remove(completed), Removal::Absent));
                        if expected == 0 {
                            wake.take().expect("completed task wake").wake();
                        }
                    }
                });
                assert_eq!(allocated, (0, 0));
                for id in ids {
                    assert!(matches!(plane.remove(id), Removal::Removed));
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}
