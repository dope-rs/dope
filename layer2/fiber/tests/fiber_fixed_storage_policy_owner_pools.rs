use dope::core::driver::life::recovery;
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;

use crate::common::runtime::profile;
pub mod common;

use std::{convert::Infallible, pin};

use dope::core::driver::route::{Epoch, KeyTag, indices::SlotIndex};
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

use crate::common::runtime::{self, Setup};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn reuse_rejects_stale() {
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
            let pool = Pool::<'_, abi::Ready<()>, 1, (), u8>::try_new()
                .expect("task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let first = plane
                    .as_mut()
                    .route::<Here>()
                    .try_admit((), parent)
                    .expect("first task insertion failed")((
                    Ready::new(()),
                    1,
                ));
                let admitted = match plane.as_mut().select(turn.turn().app()) {
                    Selection::Ready(admitted) => Some(admitted),
                    Selection::Idle | Selection::Yielded => None,
                }
                .expect("first task was not ready");
                assert!(matches!(
                    admitted.only().drive(&mut driver),
                    Outcome::Complete {
                        target: 1,
                        output: (),
                    },
                ));

                let second = plane
                    .as_mut()
                    .route::<Here>()
                    .try_admit((), parent)
                    .expect("reused task insertion failed")((
                    Ready::new(()),
                    2,
                ));
                assert!((plane.as_mut().route::<Here>().remove(first)) == (Removal::Absent));
                let admitted = match plane.as_mut().select(turn.turn().app()) {
                    Selection::Ready(admitted) => Some(admitted),
                    Selection::Idle | Selection::Yielded => None,
                }
                .expect("reused task was not ready");
                assert!(matches!(
                    admitted.only().drive(&mut driver),
                    Outcome::Complete {
                        target: 2,
                        output: (),
                    },
                ));
                assert!((plane.as_mut().route::<Here>().remove(second)) == (Removal::Absent));
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
fn rejects_another_pool() {
    let mut reports = Vec::new();

    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let ready = view
                .ready()
                .make_slot(
                    view.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let parent = RootWaker::from(ready.target());
            let left = Pool::<'_, abi::Ready<u8>, 1, (), u8>::try_new().expect("left pool");
            let right = Pool::<'_, abi::Ready<u8>, 1, (), u8>::try_new().expect("right pool");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Branch::new(
                Leaf::new(left),
                Leaf::new(right)
            )));
            Scope::with(plane.as_mut(), |mut plane| {
                let ((), allocs) = Tracker::usage(|| {
                    let id = plane
                        .as_mut()
                        .route::<Left<Here>>()
                        .try_admit((), parent)
                        .expect("left admission")((Ready::new(7), 7));
                    let other = plane
                        .as_mut()
                        .route::<Right<Here>>()
                        .try_admit((), parent)
                        .expect("right admission")((
                        Ready::new(11),
                        11,
                    ));
                    assert!(
                        (plane.as_mut().route::<Right<Here>>().remove(id)) == (Removal::Absent)
                    );
                    let selected = match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(selected) => Some(selected),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("original task remains queued");
                    assert!(matches!(
                        Routes::run(
                            Branch::new(
                                Leaf::new(|driver, entry| Entry::<_, Left<Here>>::drive(
                                    entry, driver
                                )),
                                Leaf::new(|driver, entry| Entry::<_, Right<Here>>::drive(
                                    entry, driver
                                ))
                            ),
                            &mut driver,
                            selected
                        ),
                        Outcome::Complete {
                            target: 7,
                            output: 7
                        }
                    ));
                    let selected = match plane.as_mut().select(turn.turn().app()) {
                        Selection::Ready(selected) => Some(selected),
                        Selection::Idle | Selection::Yielded => None,
                    }
                    .expect("foreign task remains queued");
                    assert!(matches!(
                        Routes::run(
                            Branch::new(
                                Leaf::new(|driver, entry| Entry::<_, Left<Here>>::drive(
                                    entry, driver
                                )),
                                Leaf::new(|driver, entry| Entry::<_, Right<Here>>::drive(
                                    entry, driver
                                ))
                            ),
                            &mut driver,
                            selected
                        ),
                        Outcome::Complete {
                            target: 11,
                            output: 11
                        }
                    ));
                    assert!(matches!(
                        plane.as_mut().route::<Right<Here>>().remove(other),
                        Removal::Absent
                    ));
                });
                assert_eq!(allocs, (0, 0));
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
fn retired_slots_stay_full() {
    for complete in [false, true] {
        let mut reports = Vec::new();
        Setup::for_profile::<profile::Tasks>().with_turn(
            |turn, mut driver| {
                let view = driver.driver_ref();
                let ready = view
                    .ready()
                    .make_slot(
                        view.targets::<KeyTag<0>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch(),
                    )
                    .expect("ready slot");
                let parent = RootWaker::from(ready.target());
                let pool =
                    Pool::<'_, Ready<u8>, 1, (), u8, 1>::try_new().expect("single generation pool");
                let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
                Scope::with(plane.as_mut(), |mut plane| {
                    let id =
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .expect("first admission")((Ready::new(17), 7));
                    if complete {
                        let selected = match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(selected) => Some(selected),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("task ready");
                        assert!(matches!(
                            selected.only().drive(&mut driver),
                            Outcome::Complete {
                                target: 7,
                                output: 17
                            }
                        ));
                        assert!(matches!(
                            plane.as_mut().route::<Here>().remove(id),
                            Removal::Absent
                        ));
                    } else {
                        assert!(matches!(
                            plane.as_mut().route::<Here>().remove(id),
                            Removal::Removed
                        ));
                    }
                    assert!(
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .is_err()
                    );
                    assert!(matches!(
                        plane.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
        assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
    }
}

#[test]
fn transfer_rearms_parent() {
    use std::{cell::Cell, num::NonZero, rc::Rc};

    use dope::core::driver::schedule::{self, app::step::Flow, ready::task::owner};
    use dope_fiber::task::storage::fixed::{quota::Physical, schedule::queue::Owned};

    use crate::common::{
        schedule::tasks::{
            fixtures::{Rounds as _, WakeProbe},
            retained::Retain as _,
        },
        storage::policies::owner::{Stamp, Tasks},
    };

    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let view = context.driver_ref();
                let targets = view.targets::<KeyTag<0>>();
                let routes = [0, 1].map(|index| {
                    targets
                        .bind(SlotIndex::from(index), Epoch::INITIAL)
                        .dispatch()
                });
                let roots = routes.map(|route| view.ready().make_slot(route).expect("parent slot"));
                let owners =
                    [0, 1].map(|lane| owner::Owner::from(Stamp::try_new(lane, 0).expect("owner")));
                let pool = Pool::<'_, WakeProbe<'_>, 4, (), usize>::try_new().expect("task pool");
                let plane = Plane::<'_, _, Physical, Owned<'_, Tasks>>::try_owned(
                    Leaf::new(pool),
                    owner::Capacity::new(NonZero::new(2).expect("owners")),
                )
                .expect("owner queue");
                let mut plane = pin::pin!(plane);
                let mut driver = context.retained();
                let polls = [0, 1, 2, 3].map(|_| Rc::new(Cell::new(0)));
                let wakes = [0, 1, 2, 3].map(|_| Rc::new(Cell::new(None)));
                Scope::with(plane.as_mut(), |mut plane| {
                    for owner in owners {
                        assert!(
                            plane.as_ref().owners().register(owner).is_ok(),
                            "registration"
                        );
                    }
                    let ids = [0, 1, 2, 3].map(|index| {
                        let owner = usize::from(index != 2);
                        plane
                            .as_mut()
                            .route::<Here>()
                            .try_admit(owners[owner], RootWaker::from(roots[owner].target()))
                            .ok()
                            .expect("task admission")((
                            WakeProbe {
                                polls: polls[index].clone(),
                                wake: wakes[index].clone(),
                            },
                            index,
                        ))
                    });
                    {
                        let turn = control.begin();
                        for owner in [1, 1, 0] {
                            let item = match plane
                                .as_mut()
                                .select(owner::Scope::Exact(owners[owner]), turn.turn().app())
                            {
                                Selection::Ready(item) => Some(item),
                                Selection::Idle | Selection::Yielded => None,
                            }
                            .expect("initial task");
                            item.only().step(&mut driver).1.settle(Flow::Yield);
                        }
                        turn.ready_round(|_| {});
                    }
                    roots[0].target().wake();
                    for wake in &wakes[..3] {
                        wake.take().expect("task wake").wake();
                    }
                    let alloc = Tracker::during(|| {
                        let mut first = true;
                        for _ in 0..4 {
                            let turn = control.begin();
                            turn.ready_round(|event| {
                                let index = routes
                                    .iter()
                                    .position(|route| route.matches(event))
                                    .expect("parent event");
                                if first {
                                    assert_eq!(
                                        index, 0,
                                        "foreign parent dispatch must be consumed first"
                                    );
                                    first = false;
                                }
                                let owner = owner::Scope::Exact(owners[index]);
                                turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                    if let Selection::Ready(item) =
                                        plane.as_mut().select(owner, share.work())
                                    {
                                        item.only().step(&mut driver).1.settle(Flow::Yield);
                                    }
                                });
                                plane.as_mut().wake(owner);
                            });
                        }
                    });
                    assert_eq!(alloc, (0, 0));
                    assert_eq!(
                        polls[2].get(),
                        2,
                        "foreign parent must dispatch after its ingress moved"
                    );
                    for id in ids {
                        assert!(matches!(
                            plane.as_mut().route::<Here>().remove(id),
                            Removal::Removed
                        ));
                    }
                    for owner in owners {
                        assert!(
                            plane.as_ref().owners().try_retire(owner).is_ok(),
                            "retirement"
                        );
                    }
                });
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

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}
