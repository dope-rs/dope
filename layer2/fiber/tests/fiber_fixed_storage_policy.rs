use dope::core::driver::{life::recovery, schedule::app::step::Flow};
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;
use o3::alloc::Error;

use crate::common::runtime::{self, profile::Tasks};
pub mod common;
use common::recover;

use std::{mem, num, pin};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::task::owner::Capacity,
};
use dope_fiber::{
    abi,
    task::storage::fixed::{
        Slab, direct,
        local::Removal,
        quota::{Limit, Physical},
        schedule::{
            Plan, Plane, Pool,
            queue::{Owned, PrepareError},
            result::{Outcome, Selection},
            selected::route::{Entry, Routes},
        },
        tree::{Branch, Here, Leaf, Left, Right, Tree},
    },
};
use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::storage, capacity::Count};

use crate::common::{
    runtime::Setup,
    storage::{policies::*, stores::Empty},
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn direct_has_no_layout_tax() {
    let (direct, base_mem) = Tracker::usage(Direct::new);
    let (shared, mux_mem) = Tracker::usage(|| {
        SharedRoute::try_new().map(|route| Multiplexed::from_routes(Leaf::new(route)))
    });
    let shared = shared.expect("schedule task pool allocation");

    assert_eq!(base_mem, (0, 0));
    assert_eq!(mux_mem.0, 1);
    assert!(mux_mem.1 > 0);
    assert!(mem::size_of_val(&direct) < mem::size_of_val(&shared));
    assert_eq!(
        mem::size_of::<SharedRoute>(),
        mem::size_of::<Pool<'static, abi::Ready<()>, 4096, (), u8>>(),
        "multiplexed app stack usage must not scale with route capacity",
    );
}

#[test]
fn heap_keeps_owner_layout() {
    type Slab1024 = Slab<'static, Empty<[u8; 4096]>, 1024, (), storage::Heap>;
    assert_eq!(
        mem::size_of::<Slab1024>(),
        mem::size_of::<Slab<'static, Empty<[u8; 4096]>, 1, (), storage::Heap>>(),
    );
    let (slots, full) = Tracker::usage(Slab1024::try_new);
    assert_eq!(full.0, 1);
    drop(slots.expect("heap task extent"));
    let (bounded, prepared) =
        Tracker::usage(|| direct::Plan::<Leaf<Slab1024>>::new(Limit::new(1)).try_prepare());
    let bounded = bounded.expect("bounded heap task plan");
    assert_eq!(prepared, (full.0, full.1 / 1024));
    for (physical, logical) in [(2, 1), (1, 2)] {
        let (plane, allocated) = Tracker::usage(|| {
            direct::Plan::<Leaf<Slab1024>>::new(Limit::new(logical))
                .try_prepare_with_capacity(Count::new(physical))
        });
        let plane = plane.expect("capped heap task plan");
        assert_eq!(mem::size_of_val(&plane), mem::size_of_val(&bounded));
        assert_eq!(allocated.0, 1);
        assert_eq!(allocated.1 * 1024, full.1 * physical as usize);
        let mut plane = pin::pin!(plane);
        Scope::with(plane.as_mut(), |mut plane| {
            let activity = Tracker::during(|| {
                assert!(plane.as_mut().route::<Here>().admit().is_some());
                assert_eq!(plane.available().get(), logical);
                let id = plane
                    .as_mut()
                    .route::<Here>()
                    .insert(Empty([0; 4096]))
                    .expect("first task");
                assert_eq!(plane.available().get(), logical - 1);
                let mut route = plane.as_mut().route::<Here>();
                assert!(route.insert(Empty([0; 4096])).is_none());
                assert!(route.remove(id) == Removal::Removed);
                let id = route.insert(Empty([0; 4096])).expect("reused slot");
                assert!(route.remove(id) == Removal::Removed);
            });
            assert_eq!(activity, (0, 0));
            assert_eq!(plane.available().get(), logical);
        });
    }
    let (empty, allocated) = Tracker::usage(|| {
        direct::Plan::<Leaf<Slab1024>>::new(Limit::new(1)).try_prepare_with_capacity(Count::EMPTY)
    });
    assert_eq!(allocated, (0, 0));
    let mut empty = pin::pin!(empty.expect("empty physical extent"));
    Scope::with(empty.as_mut(), |mut plane| {
        assert!(
            plane
                .as_mut()
                .route::<Here>()
                .insert(Empty([0; 4096]))
                .is_none()
        );
        assert_eq!(plane.available().get(), 1);
    });
}

#[test]
fn plan_rolls_back_tree() {
    type Routes = Branch<Leaf<SharedRoute>, Leaf<SharedRoute>>;

    assert_eq!(
        mem::size_of::<Plan<'static, Routes>>(),
        mem::size_of::<usize>()
    );
    let owners = Capacity::new(num::NonZero::<u32>::MIN.saturating_add(1));
    for capped in [false, true] {
        let plan = Plan::<'static, Routes>::new(Limit::new(6));
        let (prepared, activity) = Tracker::activity(|| {
            Tracker::fail_after(1, || {
                if capped {
                    plan.try_prepare_with_capacity::<()>(owners, Count::new(1))
                } else {
                    plan.try_prepare::<()>(owners)
                }
            })
        });
        assert!(matches!(
            prepared,
            Err(PrepareError::Routes(Error::Exhausted))
        ));
        assert_eq!(activity.allocations, 1);
        assert_eq!(activity.deallocations, 1);
        assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
    }
}

#[test]
fn owned_queue_failure() {
    let (route, setup) = Tracker::activity(SharedRoute::try_new);
    let route = route.expect("route setup allocation failed");
    let owners = Capacity::new(num::NonZero::<u32>::MIN);
    let (prepared, activity) = Tracker::activity(|| {
        Tracker::fail_after(0, || {
            Plane::<'static, _, Physical, Owned<()>>::try_owned(Leaf::new(route), owners)
        })
    });
    assert!(matches!(
        prepared,
        Err(PrepareError::Queue(Error::Exhausted))
    ));
    assert_eq!(activity.allocations, 0);
    assert_eq!(activity.allocated_bytes, 0);
    assert_eq!(activity.deallocations, setup.allocations);
    assert_eq!(activity.deallocated_bytes, setup.allocated_bytes);
}

#[test]
fn thirteen_routes_are_free() {
    type Route<'d> = Leaf<Pool<'d, abi::Ready<()>, 3, (), u8>>;
    type Two<'d> = Branch<Route<'d>, Route<'d>>;
    type Three<'d> = Branch<Route<'d>, Two<'d>>;
    type Four<'d> = Branch<Two<'d>, Two<'d>>;
    type Six<'d> = Branch<Three<'d>, Three<'d>>;
    type Seven<'d> = Branch<Three<'d>, Four<'d>>;
    type Thirteen<'d> = Branch<Six<'d>, Seven<'d>>;
    type First = Left<Left<Left<Here>>>;
    type Second = Left<Left<Right<Left<Here>>>>;
    type Third = Left<Left<Right<Right<Here>>>>;
    type Fourth = Left<Right<Left<Here>>>;
    type Fifth = Left<Right<Right<Left<Here>>>>;
    type Sixth = Left<Right<Right<Right<Here>>>>;
    type Middle = Right<Left<Left<Here>>>;
    type Eighth = Right<Left<Right<Left<Here>>>>;
    type Ninth = Right<Left<Right<Right<Here>>>>;
    type Tenth = Right<Right<Left<Left<Here>>>>;
    type Eleventh = Right<Right<Left<Right<Here>>>>;
    type Twelfth = Right<Right<Right<Left<Here>>>>;
    type Last = Right<Right<Right<Right<Here>>>>;
    assert_eq!(
        <Thirteen<'static> as Tree<'static>>::TASKS.nonzero().get(),
        39
    );
    assert_eq!(mem::size_of::<First>(), 0);
    assert_eq!(mem::size_of::<Middle>(), 0);
    assert_eq!(mem::size_of::<Last>(), 0);
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let ready = (ready).expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let (allocated, cleanup) = {
                let (one, one_mem) = Tracker::usage(SharedRoute::try_new);
                let one = one.expect("reference task pool");
                let (plane, allocated) = Tracker::usage(Plane::<'_, Thirteen<'_>>::try_new);
                let plane = plane.expect("schedule task tree allocation failed");
                assert_eq!(allocated.0, 13);
                assert_eq!(allocated.1, one_mem.1 * 13);
                let mut plane = pin::pin!(plane);
                Scope::with(plane.as_mut(), |mut plane| {
                    let alloc = Tracker::during(|| {
                        let first = plane
                            .as_mut()
                            .route::<First>()
                            .try_admit((), parent)
                            .expect("first leaf insertion failed")(
                            (
                            Ready::new(()),
                            10,
                        )
                        );
                        let middle = plane
                            .as_mut()
                            .route::<Middle>()
                            .try_admit((), parent)
                            .expect("middle leaf insertion failed")(
                            (
                            Ready::new(()),
                            20,
                        )
                        );
                        let last =
                            plane
                                .as_mut()
                                .route::<Last>()
                                .try_admit((), parent)
                                .expect("last leaf insertion failed")((
                                Ready::new(()),
                                30,
                            ));

                        for expected in [10, 20, 30] {
                            let admitted = match plane.as_mut().select(turn.turn().app()) {
                                Selection::Ready(admitted) => Some(admitted),
                                _ => None,
                            }
                            .expect("tree task was not ready");
                            let routes = Branch::new(
                                Branch::new(
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, First>::drive(admitted, driver)
                                        }),
                                        Branch::new(
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Second>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Third>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                        ),
                                    ),
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Fourth>::drive(admitted, driver)
                                        }),
                                        Branch::new(
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Fifth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Sixth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                        ),
                                    ),
                                ),
                                Branch::new(
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Middle>::drive(admitted, driver)
                                        }),
                                        Branch::new(
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Eighth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Ninth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                        ),
                                    ),
                                    Branch::new(
                                        Branch::new(
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Tenth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Eleventh>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                        ),
                                        Branch::new(
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Twelfth>::drive(
                                                    admitted, driver,
                                                )
                                            }),
                                            Leaf::new(|driver, admitted| {
                                                Entry::<Thirteen<'_>, Last>::drive(admitted, driver)
                                            }),
                                        ),
                                    ),
                                ),
                            );
                            assert_eq!(mem::size_of_val(&routes), 0);
                            let outcome = Routes::run(routes, &mut driver, admitted);
                            assert!(matches!(
                                outcome,
                                Outcome::Complete { target, output: () } if target == expected
                            ));
                        }
                        assert!(matches!(
                            plane.as_mut().route::<First>().remove(first),
                            Removal::Absent
                        ));
                        assert!(matches!(
                            plane.as_mut().route::<Middle>().remove(middle),
                            Removal::Absent
                        ));
                        assert!(matches!(
                            plane.as_mut().route::<Last>().remove(last),
                            Removal::Absent
                        ));
                    });

                    assert_eq!(alloc, (0, 0));
                });
                let cleanup = Tracker::begin();
                drop(one);
                ((allocated.0 + one_mem.0, allocated.1 + one_mem.1), cleanup)
            };
            assert_eq!(
                cleanup.finish(),
                alloc::Activity {
                    allocations: 0,
                    allocated_bytes: 0,
                    deallocations: allocated.0,
                    deallocated_bytes: allocated.1,
                },
            );
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
fn drive_hot_path_is_free() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let ready = (ready).expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<u16>, 3, (), u32>::try_new();
            let pool = (pool).expect("schedule task pool allocation failed");
            let mut schedule = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(schedule.as_mut(), |mut schedule| {
                let alloc = Tracker::during(|| {
                    let cases = [(23, 41), (24, 42), (25, 43)];
                    let tasks = cases.map(|(target, output)| {
                        schedule
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .expect("fixed task insertion failed")((
                            Ready::new(output),
                            target,
                        ))
                    });
                    for expected in cases {
                        let admitted = match schedule.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            _ => None,
                        }
                        .expect("scheduled task was not ready");
                        assert!(matches!(
                            admitted.only().drive(&mut driver),
                            Outcome::Complete { target, output } if (target, output) == expected
                        ));
                    }
                    for task in tasks {
                        assert!(matches!(
                            schedule.as_mut().route::<Here>().remove(task),
                            Removal::Absent
                        ));
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
fn deferred_batch_is_free() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let view = driver.driver_ref();
            let targets = view.targets::<KeyTag<0>>();
            let ready = view
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let ready = (ready).expect("ready slot allocation failed");
            let parent = RootWaker::from(ready.target());
            let pool = Pool::<'_, abi::Ready<u8>, 3, (), u8>::try_new();
            let pool = (pool).expect("schedule task pool allocation failed");
            let mut plane = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(plane.as_mut(), |mut plane| {
                let tasks = [0, 1, 2].map(|target| {
                    plane
                        .as_mut()
                        .route::<Here>()
                        .try_admit((), parent)
                        .expect("task insertion failed")((
                        Ready::new(target),
                        target,
                    ))
                });

                let alloc = Tracker::during(|| {
                    for expected in [0, 1, 2] {
                        let admitted = match plane.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            _ => None,
                        }
                        .expect("stepped task was not ready");
                        let (observed, receipt) = admitted.only().step(&mut driver);
                        assert!(matches!(
                            observed,
                            Outcome::Complete { target, output }
                                if target == expected && output == expected
                        ));
                        assert!((receipt.settle(Flow::Batch)) == (Flow::Batch));
                    }
                    for task in tasks {
                        assert!(matches!(
                            plane.as_mut().route::<Here>().remove(task),
                            Removal::Absent
                        ));
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

type Direct = Slab<'static, Empty<()>, 3, Domain>;
type SharedRoute = Pool<'static, abi::Ready<()>, 3, (), u8>;
type Multiplexed = Plane<'static, Leaf<SharedRoute>>;
