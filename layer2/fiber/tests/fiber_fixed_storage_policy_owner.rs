use std::{cell::Cell, convert::Infallible, num, pin, rc::Rc};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{
        app::step::Flow,
        ready::task::owner::{
            Capacity, Owner,
            Scope::{Any, Exact},
        },
    },
};
use dope_fiber::{
    abi::{self, Pending, Ready},
    context::wake::RootWaker,
    task::storage::{
        fixed::{
            local::Removal,
            quota::Physical,
            schedule::{
                Plane, Pool,
                queue::{Owned, Wake},
                result::{Outcome, Selection},
            },
            tree::{Here, Leaf},
        },
        scope::Scope,
    },
};
use dope_test::alloc::{self, Tracker};

use crate::common::{
    runtime::{self, Setup, profile},
    schedule::tasks::fixtures::{SelfWake, WakeProbe},
    storage::policies::{Rearm, owner::*},
};
pub mod common;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn scope_keeps_foreign() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<u8>, 4, (), u8>::try_new()
                .expect("task pool allocation failed");
            let (plane, cold) = Tracker::measure(|| {
                Plane::<'_, _, Physical, Owned<'_, Tasks>>::try_owned(
                    Leaf::new(pool),
                    Capacity::new(num::NonZero::<u32>::MIN.saturating_add(1)),
                )
            });
            assert_eq!(cold, [1, 112, 0, 0]);
            let plane = plane.expect("owner queue allocation failed");
            let mut plane = pin::pin!(plane);
            Scope::with(plane.as_mut(), |mut plane| {
                let first = Owner::from(Stamp::try_new(0, 0).expect("first owner"));
                let second = Owner::from(Stamp::try_new(1, 0).expect("second owner"));
                for owner in [first, second] {
                    assert!(
                        plane.as_ref().owners().register(owner).is_ok(),
                        "registration"
                    );
                }

                let alloc = Tracker::during(|| {
                    let mut route = plane.as_mut().route::<Here>();
                    let foreign = route.try_admit(second, parent);
                    let foreign =
                        foreign.ok().expect("foreign task insertion failed")((Ready::new(20), 20));
                    let exact = route.try_admit(first, parent);
                    let exact =
                        exact.ok().expect("exact task insertion failed")((Ready::new(10), 10));

                    for (scope, target) in [(Exact(first), 10), (Any, 20)] {
                        let admitted = match plane.as_mut().select(scope, turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("expected ready task");
                        let (observed, receipt) = admitted.only().step(&mut driver);
                        assert!(
                            matches!(observed, Outcome::Complete { target: actual, output }
                            if (actual, output) == (target, target))
                        );
                        assert!((receipt.settle(Flow::Batch)) == (Flow::Batch));
                        if target == 10 {
                            assert!(
                                plane.as_ref().owners().readiness(Exact(second)) == Wake::Ready
                            );
                        }
                    }

                    assert!(
                        plane.as_ref().owners().try_retire(first).is_ok(),
                        "retirement"
                    );
                    let next = Owner::from(Stamp::try_new(0, 1).expect("next owner"));
                    assert!(
                        plane.as_ref().owners().register(next).is_ok(),
                        "registration"
                    );
                    let mut route = plane.as_mut().route::<Here>();
                    let current = route.try_admit(next, parent);
                    let current =
                        current.ok().expect("reused owner insertion failed")((Ready::new(30), 30));
                    for stale in [foreign, exact] {
                        assert!(plane.as_mut().route::<Here>().remove(stale) == Removal::Absent);
                    }
                    assert!(
                        plane.as_ref().owners().readiness(Exact(first)) == Wake::Idle,
                        "a stale generation must not observe its recycled lane",
                    );
                    assert!(matches!(
                        plane.as_mut().select(Exact(first), turn.turn().app()),
                        Selection::Idle
                    ));
                    let admitted = match plane.as_mut().select(Exact(next), turn.turn().app()) {
                        Selection::Ready(admitted) => Some(admitted),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("expected ready task");
                    let (observed, receipt) = admitted.only().step(&mut driver);
                    assert!(matches!(
                        observed,
                        Outcome::Complete {
                            target: 30,
                            output: 30,
                        }
                    ));
                    assert!((receipt.settle(Flow::Batch)) == (Flow::Batch));
                    assert!(plane.as_mut().route::<Here>().remove(current) == Removal::Absent);
                    for owner in [next, second] {
                        assert!(
                            plane.as_ref().owners().try_retire(owner).is_ok(),
                            "retirement"
                        );
                    }
                });

                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn phase_defers_self_wakes() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool =
                Pool::<'_, SelfWake, 2, (), u8>::try_new().expect("task pool allocation failed");
            let plane = Plane::<'_, _, Physical, Owned<'_, Tasks>>::try_owned(
                Leaf::new(pool),
                Capacity::new(num::NonZero::<u32>::MIN),
            )
            .expect("owner queue allocation failed");
            let mut plane = pin::pin!(plane);
            Scope::with(plane.as_mut(), |mut plane| {
                let owner = Owner::from(Stamp::try_new(0, 0).expect("owner identity"));
                assert!(plane.as_ref().owners().register(owner).is_ok(), "registration");

                let polls = Rc::new(Cell::new(0));
                let alloc = Tracker::during(|| {
                    let mut route = plane.as_mut().route::<Here>();
                    let [first, second] = [1, 2].map(|target| {
                        route.try_admit(owner, parent).ok().expect("task insertion")((
                            SelfWake(polls.clone()),
                            target,
                        ))
                    });

                    for target in [1, 2] {
                        let admitted = match plane.as_mut().select(Exact(owner), turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("expected ready task");
                        let (observed, receipt) = admitted.only().step(&mut driver);
                        assert!(matches!(observed, Outcome::Pending { target: actual } if actual == target),
                            "self-wakes must enter the next phase");
                        assert!((receipt.settle(Flow::Yield)) == (Flow::Yield));
                    }

                    assert!((plane.as_mut().route::<Here>().remove(first)) == (Removal::Removed));
                    assert!((plane.as_mut().route::<Here>().remove(second)) == (Removal::Removed));
                    assert!(plane.as_ref().owners().try_retire(owner).is_ok(), "retirement");
                });

                assert_eq!(alloc, (0, 0));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn drive_returns_observed() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<u16>, 1, (), u32>::try_new()
                .expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let task =
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task insertion failed")((Ready::new(41), 23));

                let (observed, alloc) = Tracker::usage(|| {
                    let admitted = match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => Some(admitted),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("expected ready task");
                    let observed = admitted.only().drive(&mut driver);
                    assert!(plane.as_mut().route::<Here>().remove(task) == Removal::Absent);
                    observed
                });

                assert_eq!(alloc, (0, 0));
                assert!(matches!(
                    observed,
                    Outcome::Complete {
                        target: 23,
                        output: 41,
                    }
                ));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn pending_target_is_exact() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Pending<()>, 1, (), u32>::try_new()
                .expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let task =
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task insertion failed")((Pending::new(), 29));

                let admitted = match plane.as_mut().select(turn.turn().app()) {
                    Selection::Ready(admitted) => Some(admitted),

                    Selection::Idle | Selection::Yielded => None,
                }
                .expect("expected ready task");
                assert!(matches!(
                    admitted.only().drive(&mut driver),
                    Outcome::Pending { target: 29 }
                ));
                assert!((plane.as_mut().route::<Here>().remove(task)) == (Removal::Removed));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn ready_pending_restores() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool =
                Pool::<'_, Rearm, 1, (), u8>::try_new().expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let task = plane
                    .as_mut()
                    .route::<Here>()
                    .try_admit((), parent)
                    .expect("task insertion failed")((Rearm, 5));

                let alloc = Tracker::during(|| {
                    let admitted = match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(admitted) => Some(admitted),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("expected ready task");
                    assert!(matches!(
                        admitted.only().drive(&mut driver),
                        Outcome::PendingReady { target: 5 },
                    ));
                });

                assert_eq!(alloc, (0, 0));
                assert!((plane.as_mut().route::<Here>().remove(task)) == (Removal::Removed));
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn ingress_uses_work() {
    use dope::core::driver::schedule;

    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
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
            let pool = Pool::<'_, WakeProbe<'_>, 3, (), usize>::try_new().expect("task pool");
            let plane = Plane::<'_, _, Physical, Owned<'_, Tasks>>::try_owned(
                Leaf::new(pool),
                Capacity::new(num::NonZero::new(3).expect("owner count")),
            )
            .expect("owner queue");
            let mut plane = pin::pin!(plane);
            let polls = [0, 1, 2].map(|_| Rc::new(Cell::new(0)));
            let wakes = [0, 1, 2].map(|_| Rc::new(Cell::new(None)));
            let owners =
                [0, 1, 2].map(|lane| Owner::from(Stamp::try_new(lane, 0).expect("owner identity")));
            Scope::with(plane.as_mut(), |mut plane| {
                let ids = [0, 1, 2].map(|index| {
                    assert!(
                        plane.as_ref().owners().register(owners[index]).is_ok(),
                        "registration"
                    );
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit(owners[index], RootWaker::from(root.target()))
                        .ok()
                        .expect("task admission")((
                        WakeProbe {
                            polls: polls[index].clone(),
                            wake: wakes[index].clone(),
                        },
                        index,
                    ))
                });
                for _ in 0..3 {
                    let item = match plane.as_mut().select(Any, turn.turn().app()) {
                        Selection::Ready(item) => Some(item),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("expected ready task");
                    item.only().step(&mut driver).1.settle(Flow::Yield);
                }
                for wake in &wakes {
                    wake.take().expect("task wake").wake();
                }
                let alloc = Tracker::during(|| {
                    for _ in 0..3 {
                        assert!(
                            plane.as_ref().owners().readiness(Exact(owners[0])) == Wake::Pending
                        );
                        assert!(plane.as_mut().try_wake(Exact(owners[0])) == Wake::Pending);
                    }
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        let work = share.work();
                        assert_eq!(work.remaining(), 1);
                        for _ in 0..3 {
                            assert!(matches!(
                                plane.as_mut().select(Exact(owners[2]), work),
                                Selection::Yielded
                            ));
                            assert_eq!(work.remaining(), 0);
                            assert!(
                                plane.as_ref().owners().readiness(Exact(owners[1]))
                                    == Wake::Pending
                            );
                        }
                    });
                    for index in [0, 2] {
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            let item =
                                match plane.as_mut().select(Exact(owners[index]), share.work()) {
                                    Selection::Ready(item) => Some(item),
                                    Selection::Idle | Selection::Yielded => None,
                                }
                                .expect("expected ready task");
                            item.only().step(&mut driver).1.settle(Flow::Yield);
                            assert_eq!(share.work().remaining(), 0);
                            if index == 0 {
                                assert!(
                                    plane.as_ref().owners().readiness(Exact(owners[1]))
                                        == Wake::Ready
                                );
                                assert!(
                                    plane.as_ref().owners().readiness(Exact(owners[2]))
                                        == Wake::Pending
                                );
                            } else {
                                assert_eq!(polls[2].get(), 2);
                            }
                        });
                    }
                });
                assert_eq!(alloc, (0, 0));
                for (id, owner) in ids.into_iter().zip(owners) {
                    assert!(plane.as_mut().route::<Here>().remove(id) == Removal::Removed);
                    assert!(
                        plane.as_ref().owners().try_retire(owner).is_ok(),
                        "retirement"
                    );
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
