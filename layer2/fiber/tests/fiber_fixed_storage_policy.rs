use dope::core::driver::{life::recovery, schedule::app::step::Flow};
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;
use o3::alloc::Error;

use crate::common::runtime::{self, profile::Tasks};
pub mod common;

use std::{convert::Infallible, mem, num, pin};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::task::owner::Capacity,
};
use dope_fiber::{
    abi,
    task::storage::fixed::{
        direct,
        local::Removal,
        quota::{Limit, Physical},
        schedule::{
            Plan, Plane, Pool,
            queue::{Owned, PrepareError},
            result::{Outcome, Selection},
            selected::route::{Entry, Routes},
        },
        staged::Slab,
        tree::{At, Branch, Here, Leaf, Left, Right, Tree},
    },
};
use dope_test::alloc::Tracker;
use o3::depot::arena::slab::storage;

use crate::common::{runtime::Setup, storage::policies::*};

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
    type Slab1024 = Slab<'static, Ready<[u8; 4096]>, 1024, (), storage::Heap>;
    assert_eq!(
        mem::size_of::<Slab1024>(),
        mem::size_of::<Slab<'static, Ready<[u8; 4096]>, 1, (), storage::Heap>>(),
    );
    let (slots, allocs) = Tracker::count(Slab1024::try_new);
    assert_eq!(allocs, 1);
    let mut plane = pin::pin!(direct::Plane::bounded(
        Leaf::new(slots.expect("heap task extent")),
        Limit::new(1),
    ));
    Scope::with(plane.as_mut(), |mut plane| {
        let activity = Tracker::during(|| {
            let mut route = plane.as_mut().route::<Here>();
            let id = route.insert(Ready::new([0; 4096])).expect("first task");
            assert!(route.insert(Ready::new([0; 4096])).is_none());
            assert!(route.remove(id) == Removal::Removed);
            let id = route.insert(Ready::new([0; 4096])).expect("reused slot");
            assert!(route.remove(id) == Removal::Removed);
        });
        assert_eq!(activity, (0, 0));
    });
}

#[test]
fn routes_share_one_reserve() {
    let (one_mem, alloc, measure) = {
        let (route, one_mem) = Tracker::usage(SharedRoute::try_new);
        assert!(route.is_ok(), "schedule task pool allocation");
        let (plane, alloc) = Tracker::usage(|| {
            let left = SharedRoute::try_new()?;
            let right = SharedRoute::try_new()?;
            Ok::<_, Error>(Plane::<'static, _>::from_routes(Branch::new(
                Leaf::new(left),
                Leaf::new(right),
            )))
        });
        assert!(plane.is_ok(), "task tree allocation");
        assert_eq!(alloc.0, one_mem.0 * 2);
        assert_eq!(alloc.1, one_mem.1 * 2);
        (one_mem, alloc, Tracker::begin())
    };
    assert_eq!(
        measure.finish(),
        alloc::Activity {
            allocations: 0,
            allocated_bytes: 0,
            deallocations: one_mem.0 + alloc.0,
            deallocated_bytes: one_mem.1 + alloc.1,
        }
    );
}

#[test]
fn plan_rolls_back_tree() {
    type Routes = Branch<Leaf<SharedRoute>, Leaf<SharedRoute>>;

    assert_eq!(
        mem::size_of::<Plan<'static, Routes>>(),
        mem::size_of::<usize>()
    );
    let plan = Plan::<'static, Routes>::new(Limit::new(6));
    let owners = Capacity::new(num::NonZero::<u32>::MIN.saturating_add(1));
    let (prepared, activity) =
        Tracker::activity(|| Tracker::fail_after(1, || plan.try_prepare::<()>(owners)));
    assert!(matches!(
        prepared,
        Err(PrepareError::Routes(Error::Exhausted))
    ));
    assert_eq!(activity.allocations, 1);
    assert_eq!(activity.deallocations, 1);
    assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
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
fn tree_has_thirteen_leaves() {
    type Route = Leaf<SharedRoute>;
    type Two = Branch<Route, Route>;
    type Three = Branch<Route, Two>;
    type Four = Branch<Two, Two>;
    type Six = Branch<Three, Three>;
    type Seven = Branch<Three, Four>;
    type Thirteen = Branch<Six, Seven>;
    type First = Left<Left<Left<Here>>>;
    type Middle = Right<Left<Left<Here>>>;
    type Last = Right<Right<Right<Right<Here>>>>;

    assert_eq!(<Thirteen as Tree<'static>>::TASKS.nonzero().get(), 39);
    assert_eq!(<Thirteen as At<'static, First>>::OFFSET.into_u32(), 0);
    assert_eq!(<Thirteen as At<'static, Middle>>::OFFSET.into_u32(), 18);
    assert_eq!(<Thirteen as At<'static, Last>>::OFFSET.into_u32(), 36);
    assert_eq!(mem::size_of::<First>(), 0);
    assert_eq!(mem::size_of::<Middle>(), 0);
    assert_eq!(mem::size_of::<Last>(), 0);

    let (alloc, measure) = {
        let (plane, alloc) = Tracker::usage(Plane::<'static, Thirteen>::try_new);
        assert!(plane.is_ok(), "schedule task tree allocation");
        assert_eq!(alloc.0, 13, "construction reserves every fixed pool");
        (alloc, Tracker::begin())
    };
    assert_eq!(
        measure.finish(),
        alloc::Activity {
            allocations: 0,
            allocated_bytes: 0,
            deallocations: alloc.0,
            deallocated_bytes: alloc.1,
        }
    );
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
            let plane = Plane::<'_, Thirteen<'_>>::try_new();
            let plane = (plane).expect("schedule task tree allocation failed");
            let mut plane = pin::pin!(plane);
            Scope::with(plane.as_mut(), |mut plane| {
                let alloc = Tracker::during(|| {
                    let first = plane
                        .as_mut()
                        .route::<First>()
                        .try_admit((), parent)
                        .expect("first leaf insertion failed")((
                        Ready::new(()),
                        10,
                    ));
                    let middle = plane
                        .as_mut()
                        .route::<Middle>()
                        .try_admit((), parent)
                        .expect("middle leaf insertion failed")((
                        Ready::new(()),
                        20,
                    ));
                    let last = plane
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
                                            Entry::<Thirteen<'_>, Second>::drive(admitted, driver)
                                        }),
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Third>::drive(admitted, driver)
                                        }),
                                    ),
                                ),
                                Branch::new(
                                    Leaf::new(|driver, admitted| {
                                        Entry::<Thirteen<'_>, Fourth>::drive(admitted, driver)
                                    }),
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Fifth>::drive(admitted, driver)
                                        }),
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Sixth>::drive(admitted, driver)
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
                                            Entry::<Thirteen<'_>, Eighth>::drive(admitted, driver)
                                        }),
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Ninth>::drive(admitted, driver)
                                        }),
                                    ),
                                ),
                                Branch::new(
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Tenth>::drive(admitted, driver)
                                        }),
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Eleventh>::drive(admitted, driver)
                                        }),
                                    ),
                                    Branch::new(
                                        Leaf::new(|driver, admitted| {
                                            Entry::<Thirteen<'_>, Twelfth>::drive(admitted, driver)
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
            let pool = Pool::<'_, abi::Ready<()>, 3, (), u8>::try_new();
            let pool = (pool).expect("schedule task pool allocation failed");
            let mut schedule = pin::pin!(Plane::<'_, _>::from_routes(Leaf::new(pool)));
            Scope::with(schedule.as_mut(), |mut schedule| {
                let mut done = 0;

                let alloc = Tracker::during(|| {
                    let tasks = [0, 1, 2].map(|target| {
                        schedule
                            .as_mut()
                            .route::<Here>()
                            .try_admit((), parent)
                            .expect("fixed task insertion failed")((
                            Ready::new(()),
                            target,
                        ))
                    });
                    for _ in 0..3 {
                        let admitted = match schedule.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            _ => None,
                        }
                        .expect("scheduled task was not ready");
                        assert!(matches!(
                            admitted.only().drive(&mut driver),
                            Outcome::Complete { output: (), .. }
                        ));
                        done += 1;
                    }
                    for task in tasks {
                        assert!(matches!(
                            schedule.as_mut().route::<Here>().remove(task),
                            Removal::Absent
                        ));
                    }
                });

                assert_eq!(alloc, (0, 0));
                assert_eq!(done, 3);
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
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

type Direct = Slab<'static, abi::Ready<()>, 3, Domain>;
type SharedRoute = Pool<'static, abi::Ready<()>, 3, (), u8>;
type Multiplexed = Plane<'static, Leaf<SharedRoute>>;

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}
