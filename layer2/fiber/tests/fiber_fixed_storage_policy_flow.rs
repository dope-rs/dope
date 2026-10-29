use dope::core::driver::life::recovery;
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;

use crate::common::runtime::profile::Tasks;
pub mod common;

use std::{cell::Cell, convert::Infallible, mem, pin, rc::Rc};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{self, app::step::Flow, ready::Readiness},
};
use dope_fiber::{
    abi,
    task::storage::fixed::{
        local::Removal,
        schedule::{
            Plane, Pool,
            result::{Outcome, Selection},
            selected::route::{Entry, Routes},
        },
        tree::{Branch, Here, Leaf, Left, Right},
    },
};
use dope_test::alloc::Tracker;

use crate::common::{
    runtime::{self, Setup},
    schedule::tasks::fixtures::{CaptureReady, WakeProbe},
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
            let pool = Pool::<'_, abi::Ready<()>, 1, (), u8>::try_new();
            let pool = pool.expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool,)));
            Scope::with(plane.as_mut(), |mut plane| {
                let task =
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task insertion failed")((Ready::new(()), 7));

                let alloc = Tracker::during(|| {
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => admitted.only().target() == 7,
                        Selection::Idle | Selection::Yielded => false,
                    });
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => {
                            let admitted = admitted.only();
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
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(task),
                        Removal::Absent
                    ));
                });

                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
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
            let pool = Pool::<'_, abi::Ready<()>, 3, (), u8>::try_new().expect("task pool");
            let mut plane = pin::pin!(Plane::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let alloc = Tracker::during(|| {
                    let [first, second, _third] = [1, 2, 3].map(|target| {
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .expect("task admission")((
                            Ready::new(()),
                            target,
                        ))
                    });
                    assert!(match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(selected) => {
                            let selected = selected.only();
                            assert_eq!(selected.target(), 1);
                            mem::forget(selected);
                            true
                        }
                        Selection::Idle | Selection::Yielded => false,
                    });
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(second),
                        Removal::Removed
                    ));
                    let _reused = plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("replacement admission")((
                        Ready::new(()),
                        4,
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(first),
                        Removal::Removed
                    ));
                    for expected in [3, 4] {
                        assert!(match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(selected) => {
                                let selected = selected.only();
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
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn drop_preserves_routes() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = (view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch()))
            .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let first = Pool::<'_, abi::Ready<()>, 2, (), u8>::try_new().expect("first task pool");
            let middle =
                Pool::<'_, abi::Ready<()>, 3, (), u8>::try_new().expect("middle task pool");
            let last = Pool::<'_, abi::Ready<()>, 4, (), u8>::try_new().expect("last task pool");
            let mut plane = pin::pin!(Plane::from_routes(Branch::new(
                Leaf::new(first),
                Branch::new(Leaf::new(middle), Leaf::new(last)),
            )));
            Scope::with(plane.as_mut(), |mut plane| {
                let first = plane
                    .as_mut()
                    .route::<Left<Here>>()
                    .try_admit((), parent)
                    .expect("task admission")((Ready::new(()), 10));
                let middle = plane
                    .as_mut()
                    .route::<Right<Left<Here>>>()
                    .try_admit((), parent)
                    .expect("task admission")((Ready::new(()), 20));
                let last = plane
                    .as_mut()
                    .route::<Right<Right<Here>>>()
                    .try_admit((), parent)
                    .expect("task admission")((Ready::new(()), 30));

                let alloc = Tracker::during(|| {
                    for _ in 0..3 {
                        let admitted = plane.as_mut().select(turn.turn().app());
                        assert!(
                            matches!(admitted, Selection::Ready(_)),
                            "root selection was not ready"
                        );
                    }
                    for expected in [10, 20, 30] {
                        let admitted = match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("restored root was not ready");
                        let observed = Routes::run(
                            Branch::new(
                                Leaf::new(|(), entry| Entry::<_, Left<Here>>::target(&entry)),
                                Branch::new(
                                    Leaf::new(|(), entry| {
                                        Entry::<_, Right<Left<Here>>>::target(&entry)
                                    }),
                                    Leaf::new(|(), entry| {
                                        Entry::<_, Right<Right<Here>>>::target(&entry)
                                    }),
                                ),
                            ),
                            (),
                            admitted,
                        );
                        assert_eq!(observed, expected);
                    }
                    for expected in [10, 20, 30] {
                        let admitted = match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("restored route was not ready");
                        let outcome = Routes::run(
                            Branch::new(
                                Leaf::new(|driver, entry| {
                                    Entry::<_, Left<Here>>::drive(entry, driver)
                                }),
                                Branch::new(
                                    Leaf::new(|driver, entry| {
                                        Entry::<_, Right<Left<Here>>>::drive(entry, driver)
                                    }),
                                    Leaf::new(|driver, entry| {
                                        Entry::<_, Right<Right<Here>>>::drive(entry, driver)
                                    }),
                                ),
                            ),
                            &mut driver,
                            admitted,
                        );
                        assert!(matches!(
                            outcome,
                            Outcome::Complete {
                                target,
                                output: ()
                            } if target == expected
                        ));
                    }
                    assert!(matches!(
                        plane.as_mut().route::<Left<Here>>().remove(first),
                        Removal::Absent,
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Right<Left<Here>>>().remove(middle),
                        Removal::Absent,
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Right<Right<Here>>>().remove(last),
                        Removal::Absent,
                    ));
                });
                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
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
            let pool = Pool::<'_, abi::Ready<()>, 1, (), u8>::try_new();
            let pool = (pool).expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool,)));
            Scope::with(plane.as_mut(), |mut plane| {
                let task =
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task insertion failed")((Ready::new(()), 9));
                while turn.turn().app().permit().is_some() {}

                assert!(matches!(
                    plane.as_mut().select(turn.turn().app()),
                    Selection::Yielded
                ));
                assert!(matches!(
                    plane.as_mut().route::<Here>().remove(task),
                    Removal::Removed
                ));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
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
            let pending = Pool::<'_, WakeProbe<'_>, 1, (), u8>::try_new().expect("pending pool");
            let rearming = Pool::<'_, Rearm, 1, (), u8>::try_new().expect("rearming pool");
            let mut plane = pin::pin!(Plane::from_routes(Branch::new(
                Leaf::new(pending),
                Leaf::new(rearming),
            )));
            let polls = Rc::new(Cell::new(0));
            let wake = Rc::new(Cell::new(None));
            Scope::with(plane.as_mut(), |mut plane| {
                let pending = plane
                    .as_mut()
                    .route::<Left<Here>>()
                    .try_admit((), parent)
                    .expect("pending admission")((
                    WakeProbe {
                        polls: polls.clone(),
                        wake: wake.clone(),
                    },
                    0,
                ));
                let rearming = plane
                    .as_mut()
                    .route::<Right<Here>>()
                    .try_admit((), parent)
                    .expect("rearming admission")((Rearm, 1));

                let allocated = Tracker::during(|| {
                    for expected in [0, 1, 0, 1, 0] {
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            let Selection::Ready(selected) = plane.as_mut().select(share.work())
                            else {
                                panic!("ready task");
                            };
                            let outcome = Routes::run(
                                Branch::new(
                                    Leaf::new(|driver, entry| {
                                        Entry::<_, Left<Here>>::drive(entry, driver)
                                    }),
                                    Leaf::new(|driver, entry| {
                                        Entry::<_, Right<Here>>::drive(entry, driver)
                                    }),
                                ),
                                &mut driver,
                                selected,
                            );
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
                assert!(matches!(
                    plane.as_mut().route::<Left<Here>>().remove(pending),
                    Removal::Removed
                ));
                assert!(matches!(
                    plane.as_mut().route::<Right<Here>>().remove(rearming),
                    Removal::Removed
                ));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
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
            let pool = Pool::<'_, CaptureReady<'_>, 2, (), u8>::try_new().expect("task pool");
            let mut plane = pin::pin!(Plane::from_routes(Leaf::new(pool)));
            let polls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let allowed = Rc::new(Cell::new(false));
            let wake = Rc::new(Cell::new(None));
            let other_wake = Rc::new(Cell::new(None));
            Scope::with(plane.as_mut(), |mut plane| {
                let mut ids = [0, 1].map(|target| {
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task admission")((
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
                let (outcome, receipt) = selected.only().step(&mut driver);
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
                        let (outcome, receipt) = selected.only().step(&mut driver);
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
                        let replacement =
                            plane
                                .as_mut()
                                .route::<Here>()
                                .try_admit((), parent)
                                .expect("replacement admission")((
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
                        assert!(matches!(
                            plane.as_mut().route::<Here>().remove(completed),
                            Removal::Absent
                        ));
                        if expected == 0 {
                            wake.take().expect("completed task wake").wake();
                        }
                    }
                });
                assert_eq!(allocated, (0, 0));
                for id in ids {
                    assert!(matches!(
                        plane.as_mut().route::<Here>().remove(id),
                        Removal::Removed
                    ));
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}
