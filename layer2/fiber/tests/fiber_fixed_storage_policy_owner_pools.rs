use dope::core::driver::life::recovery;
use dope_fiber::{abi::Ready, context::wake::RootWaker, task::storage::scope::Scope};
use dope_test::alloc;

use crate::common::runtime::profile;
pub mod common;
use common::recover;

use std::{num::NonZero, pin};

use dope::core::driver::{
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{
        app::step::Flow,
        ready::task::{self, owner},
    },
};
use dope_fiber::task::storage::fixed::{
    local::Removal,
    quota::Limit,
    schedule::{
        Plan, Plane, Pool, queue,
        result::{Outcome, Selection},
        selected::route::{Entry, Routes},
    },
    tree::{Branch, Here, Leaf, Left, Right},
};
use dope_test::alloc::Tracker;
use o3::depot::capacity::Count;

use crate::common::{
    runtime::{self, Setup},
    storage::policies::owner::{Stamp, Tasks},
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn rejects_another_pool() {
    type TaskRoutes<'d> =
        Branch<Leaf<Pool<'d, Ready<u8>, 16384, (), u8>>, Leaf<Pool<'d, Ready<u8>, 1, (), u8>>>;
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
            let owners = owner::Capacity::new(NonZero::<u32>::MIN);
            let (full, full_memory) = Tracker::usage(|| {
                Plan::<'_, TaskRoutes<'_>>::new(Limit::new(2)).try_prepare::<Tasks>(owners)
            });
            drop(full.expect("declared task extents"));
            let (plane, memory) = Tracker::usage(|| {
                Plan::<'_, TaskRoutes<'_>>::new(Limit::new(2))
                    .try_prepare_with_capacity::<Tasks>(owners, Count::new(1))
            });
            assert_eq!(memory.0, full_memory.0);
            assert!(memory.1 < full_memory.1);
            let mut plane = pin::pin!(plane.expect("physical task extents"));
            Scope::with(plane.as_mut(), |mut plane| {
                let owner = owner::Owner::from(Stamp::try_new(0, 0).expect("owner"));
                assert!(plane.as_ref().owners().register(owner).is_ok());
                let ((), allocs) = Tracker::usage(|| {
                    let id = plane
                        .as_mut()
                        .route::<Left<Here>>()
                        .try_admit(owner, parent)
                        .ok()
                        .expect("left admission")((Ready::new(7), 7));
                    assert!(matches!(
                        plane
                            .as_mut()
                            .route::<Left<Here>>()
                            .try_admit(owner, parent),
                        Err(queue::Error::Task(task::Error::Capacity { requested, available: 0 }))
                        if requested == NonZero::<usize>::MIN
                    ));
                    let other = plane
                        .as_mut()
                        .route::<Right<Here>>()
                        .try_admit(owner, parent)
                        .ok()
                        .expect("right admission")((
                        Ready::new(11),
                        11,
                    ));
                    assert!(
                        (plane.as_mut().route::<Right<Here>>().remove(id)) == (Removal::Absent)
                    );
                    for expected in [7, 11] {
                        let selected = match plane
                            .as_mut()
                            .select(owner::Scope::Exact(owner), turn.turn().app())
                        {
                            Selection::Ready(selected) => Some(selected),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("task remains queued");
                        let (outcome, receipt) = Routes::run(
                            Branch::new(
                                Leaf::new(|driver, entry| {
                                    Entry::<_, Left<Here>, _, _>::step(entry, driver)
                                }),
                                Leaf::new(|driver, entry| {
                                    Entry::<_, Right<Here>, _, _>::step(entry, driver)
                                }),
                            ),
                            &mut driver,
                            selected,
                        );
                        assert!(matches!(
                            outcome,
                            Outcome::Complete { target, output }
                            if (target, output) == (expected, expected)
                        ));
                        receipt.settle(Flow::Batch);
                    }
                    assert!(matches!(
                        plane.as_mut().route::<Right<Here>>().remove(other),
                        Removal::Absent
                    ));
                    assert!(plane.as_ref().owners().try_retire(owner).is_ok());
                });
                assert_eq!(allocs, (0, 0));
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
fn retired_slots_stay_full() {
    type TaskPool<'d> = Leaf<Pool<'d, Ready<u8>, 2, (), u8, 1>>;
    for (physical, capacity) in [(None, 2), (Some(Count::new(1)), 1)] {
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
                    let owners = owner::Capacity::new(NonZero::<u32>::MIN);
                    let plan = Plan::<'_, TaskPool<'_>>::new(Limit::new(1));
                    let plane = match physical {
                        None => plan.try_prepare::<Tasks>(owners),
                        Some(physical) => plan.try_prepare_with_capacity::<Tasks>(owners, physical),
                    }
                    .expect("single generation plane");
                    let mut plane = pin::pin!(plane);
                    Scope::with(plane.as_mut(), |mut plane| {
                        let owner = owner::Owner::from(Stamp::try_new(0, 0).expect("owner"));
                        assert!(plane.as_ref().owners().register(owner).is_ok());
                        let activity = Tracker::during(|| {
                            for _ in 0..capacity {
                                let id = plane
                                    .as_mut()
                                    .route::<Here>()
                                    .try_admit(owner, parent)
                                    .ok()
                                    .expect("remaining physical identity")((Ready::new(17), 7));
                                assert!(matches!(
                                    plane
                                        .as_mut()
                                        .route::<Here>()
                                        .try_admit(owner, parent),
                                    Err(queue::Error::Task(task::Error::Capacity { requested, available: 0 }))
                                    if requested == NonZero::<usize>::MIN
                                ));
                                if complete {
                                    let selected = match plane.as_mut().select(
                                        owner::Scope::Exact(owner),
                                        turn.turn().app(),
                                    ) {
                                        Selection::Ready(selected) => Some(selected),
                                        Selection::Idle | Selection::Yielded => None,
                                    }
                                    .expect("task ready");
                                    let (outcome, receipt) = selected.only().step(&mut driver);
                                    assert!(matches!(
                                        outcome,
                                        Outcome::Complete { target: 7, output: 17 }
                                    ));
                                    receipt.settle(Flow::Batch);
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
                            }
                            assert!(matches!(
                                plane
                                    .as_mut()
                                    .route::<Here>()
                                    .try_admit(owner, parent),
                                Err(queue::Error::Task(task::Error::Capacity { requested, available: 0 }))
                                if requested == NonZero::<usize>::MIN
                            ));
                            assert!(matches!(
                                plane.as_mut().select(owner::Scope::Exact(owner), turn.turn().app()),
                                Selection::Idle
                            ));
                            assert!(plane.as_ref().owners().try_retire(owner).is_ok());
                        });
                        assert_eq!(activity, (0, 0));
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                runtime::check_scope,
            );
            assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
        }
    }
}

#[test]
fn transfer_rearms_parent() {
    use std::{cell::Cell, num::NonZero, rc::Rc};

    use dope::core::driver::schedule::{self, app::step::Flow, ready::task::owner};
    use dope_fiber::task::storage::fixed::{quota::Physical, schedule::queue::Owned};

    use crate::common::{
        schedule::tasks::fixtures::{Rounds as _, WakeProbe},
        storage::policies::owner::{Stamp, Tasks},
    };

    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            let (_, context, mut control) = scope.split();
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
            let mut driver = context;
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
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
