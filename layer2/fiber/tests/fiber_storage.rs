use dope_fiber::task::storage::{
    fixed::{
        Capacity,
        direct::Plane,
        local::Removal,
        staged::{Poll, State, Store as _},
        tree::{Branch, Here, Leaf, Left, Right},
    },
    scope::Scope,
};

use crate::common::runtime::profile;
pub mod common;

use std::{cell::Cell, convert::Infallible, mem::size_of, pin::pin, rc::Rc, task};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    abi::Ready,
    context::Context,
    task::storage::{
        Id, fixed,
        fixed::{local, local::Store, staged},
    },
};
use dope_test::alloc::Tracker;
use fixed::quota::Limit;
use o3::depot::arena::slab::key;

use crate::common::{
    runtime::{self, Setup},
    storage::stores::*,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn staged_take_once() {
    let mut reports = Vec::new();
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let polls = Rc::new(Cell::new(0));
            let mut slab = pin!(staged::Slab::<'_, CompleteOnce, 1, Domain>::new());
            Scope::with(slab.as_mut(), |mut slab| {
                let id = slab
                    .as_mut()
                    .insert(CompleteOnce {
                        polls: Rc::clone(&polls),
                    })
                    .expect("staged task slot");
                let first = Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| slab.as_mut().poll_staged(&id, context.as_mut()),
                );
                assert!(matches!(first, Some(Poll::NewlyReady)));
                assert_eq!(slab.as_mut().take_output(&id), Some(41));
                assert!(slab.state(&id) == Some(State::Vacant));
                assert!(slab.capacity() == Capacity::Full);
                let second = Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| slab.as_mut().poll_staged(&id, context.as_mut()),
                );
                assert!(second.is_none());
                assert_eq!(polls.get(), 1);
                assert_eq!(slab.as_mut().take_output(&id), None);
                assert!(slab.as_mut().remove(id) == Removal::Removed);
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(reports.is_empty(), "unexpected reclamations: {reports:?}");
}

#[test]
fn keeps_pinned_on_drop() {
    let mut reports = Vec::new();

    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = root.expect("ready slot");
            let observed = Rc::new(Cell::new((0, true)));
            {
                let mut slab = pin!(staged::Slab::<'_, _, 1, Domain>::new());
                Scope::with(slab.as_mut(), |mut slab| {
                    let task = slab.as_mut().insert(Pinned {
                        address: Cell::new(None),
                        observed: Rc::clone(&observed),
                    });
                    let task = task.expect("task slot");
                    let poll = Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |mut context| slab.as_mut().poll_staged(&task, context.as_mut()),
                    );
                    assert!(matches!(poll, Some(Poll::Pending)));
                });
            }
            assert_eq!(observed.get(), (1, true));
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
fn nominal_ids_keep_layout() {
    let key = size_of::<key::Parts>();
    assert_eq!(size_of::<Id<'static, u8>>(), 2 * key);
    assert_eq!(size_of::<Option<Id<'static, u8>>>(), 2 * key);
    assert_eq!(size_of::<Result<(), Id<'static, u8>>>(), 2 * key);
    assert_eq!(size_of::<local::Removal>(), 1);

    let mut left = pin!(staged::Slab::<'static, _, 1, u8>::new());
    Scope::with(left.as_mut(), |mut left| {
        let mut right = pin!(staged::Slab::<'static, _, 1, u16>::new());
        Scope::with(right.as_mut(), |mut right| {
            assert!((Store::capacity(left.as_ref().get_ref())) == (Capacity::Available));
            let left_id = (left.as_mut().insert(Ready::new(()))).expect("left task slot");
            assert!((Store::capacity(left.as_ref().get_ref())) == (Capacity::Full));
            let right_id = (right.as_mut().insert(Ready::new(()))).expect("right task slot");
            assert!((left.as_mut().remove(left_id)) == (Removal::Removed));
            assert!((Store::capacity(left.as_ref().get_ref())) == (Capacity::Available));
            assert!((right.as_mut().remove(right_id)) == (Removal::Removed));
        });
    });
}

#[test]
fn rejection_returns_id() {
    let owner = staged::Slab::<'static, Ready<()>, 1, Domain>::new();
    let foreign = staged::Slab::<'static, Ready<()>, 1, Domain>::new();
    let mut stores = pin!(Plane::new(Branch::new(
        Leaf::new(owner),
        Leaf::new(foreign)
    )));
    Scope::with(stores.as_mut(), |mut stores| {
        let id = stores
            .as_mut()
            .route::<Left<Here>>()
            .insert(Ready::new(()))
            .expect("owner task slot");
        let other = stores
            .as_mut()
            .route::<Right<Here>>()
            .insert(Ready::new(()))
            .expect("foreign task slot");
        let result = stores
            .as_mut()
            .route::<Right<Here>>()
            .as_mut()
            .try_remove(id);
        let id = result.expect_err("foreign storage must reject the task identity");
        assert!(stores.as_mut().route::<Left<Here>>().remove(id) == Removal::Removed);
        assert!(stores.as_mut().route::<Right<Here>>().remove(other) == Removal::Removed);
    });
}

#[test]
fn staging_keeps_output() {
    let mut reports = Vec::new();

    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = root.expect("ready slot");
            let polls = Rc::new(Cell::new(0));
            let slab = staged::Slab::<'_, CompleteOnce, 1, Domain>::new();
            let foreign = staged::Slab::<'_, CompleteOnce, 1, Domain>::new();
            let mut stores = pin!(Plane::new(Branch::new(Leaf::new(slab), Leaf::new(foreign))));
            Scope::with(stores.as_mut(), |mut stores| {
                let task = stores
                    .as_mut()
                    .route::<Left<Here>>()
                    .as_mut()
                    .insert(CompleteOnce {
                        polls: Rc::clone(&polls),
                    });
                let task = task.expect("staged task slot");
                let visits = Rc::new(Cell::new(0));
                let other = stores
                    .as_mut()
                    .route::<Right<Here>>()
                    .as_mut()
                    .insert(CompleteOnce {
                        polls: Rc::clone(&visits),
                    })
                    .expect("foreign staged task slot");

                let ((), allocs) = Tracker::usage(|| {
                    let rejected = Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |mut context| {
                            stores
                                .as_mut()
                                .route::<Right<Here>>()
                                .as_mut()
                                .poll_staged(&task, context.as_mut())
                        },
                    );
                    assert!(rejected.is_none());
                    assert!(
                        stores
                            .as_mut()
                            .route::<Right<Here>>()
                            .state(&task)
                            .is_none()
                    );
                    assert_eq!(
                        stores
                            .as_mut()
                            .route::<Right<Here>>()
                            .as_mut()
                            .take_output(&task),
                        None
                    );
                    assert_eq!(visits.get(), 0);
                    let first = Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |mut context| {
                            stores
                                .as_mut()
                                .route::<Left<Here>>()
                                .as_mut()
                                .poll_staged(&task, context.as_mut())
                        },
                    );
                    assert!((first) == (Some(Poll::NewlyReady)));
                    assert!(
                        (stores.as_mut().route::<Left<Here>>().state(&task))
                            == (Some(State::Staged))
                    );

                    let second = Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |mut context| {
                            stores
                                .as_mut()
                                .route::<Left<Here>>()
                                .as_mut()
                                .poll_staged(&task, context.as_mut())
                        },
                    );
                    assert!((second) == (Some(Poll::Staged)));
                    assert_eq!(polls.get(), 1);
                    assert_eq!(
                        stores
                            .as_mut()
                            .route::<Left<Here>>()
                            .as_mut()
                            .take_output(&task),
                        Some(41)
                    );
                    assert!(
                        (stores.as_mut().route::<Left<Here>>().state(&task))
                            == (Some(State::Vacant))
                    );
                });
                assert_eq!(allocs, (0, 0));
                let result = stores
                    .as_mut()
                    .route::<Right<Here>>()
                    .as_mut()
                    .try_remove(task);
                assert!(result.is_err(), "foreign staged owner");
                let Err(task) = result else {
                    return;
                };
                assert!(
                    (stores
                        .as_mut()
                        .route::<Right<Here>>()
                        .as_mut()
                        .remove(other))
                        == (Removal::Removed)
                );
                assert!(
                    (Store::remove(stores.as_mut().route::<Left<Here>>().as_mut(), task))
                        == (Removal::Removed)
                );
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
fn removal_drops_output() {
    let mut reports = Vec::new();

    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = root.expect("ready slot");
            let drops = Rc::new(Cell::new(0));
            let mut slab = pin!(staged::Slab::<'_, _, 1, Domain>::new());
            Scope::with(slab.as_mut(), |mut slab| {
                let task = slab.as_mut().insert(DropReady {
                    output_drops: Rc::clone(&drops),
                });
                let task = task.expect("staged task slot");

                let poll = Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| slab.as_mut().poll_staged(&task, context.as_mut()),
                );
                assert!((poll) == (Some(Poll::NewlyReady)));
                assert_eq!(drops.get(), 0);
                assert!((Store::remove(slab.as_mut(), task)) == (Removal::Removed));
                assert_eq!(drops.get(), 1);
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
fn deferred_completion() {
    use staged::Completion as _;

    let mut report = None;
    Setup::for_profile::<profile::Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let target = core
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let root = core.ready().make_slot(target).expect("ready slot");
            let polls = Rc::new(Cell::new(0));
            let slab = staged::Slab::<'_, CompleteOnce, 1, Domain>::new();
            let mut plane = pin!(Plane::bounded(Leaf::new(slab), Limit::new(1)));
            Scope::with(plane.as_mut(), |mut scope| {
                let mut route = scope.as_mut().route::<Here>();
                let id = route
                    .as_mut()
                    .insert(CompleteOnce {
                        polls: polls.clone(),
                    })
                    .expect("task slot");
                Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| {
                        {
                            let ready = route
                                .as_mut()
                                .poll_ready(&id, context.as_mut())
                                .expect("live task");
                            assert!(matches!(ready, task::Poll::Ready((_, true))));
                        }
                        let ready = route
                            .as_mut()
                            .poll_ready(&id, context.as_mut())
                            .expect("staged task");
                        let (ready, fresh) = match ready {
                            task::Poll::Ready(ready) => Some(ready),
                            task::Poll::Pending => None,
                        }
                        .expect("staged completion");
                        assert!(!fresh);
                        assert_eq!(ready.finish(), 41);
                    },
                );
                assert_eq!(polls.get(), 1);
                assert!(route.as_mut().remove(id) == Removal::Absent);
                assert!(route.capacity() == fixed::Capacity::Available);
            });
            assert_eq!(plane.available().get(), 1);
        },
        recovery::Policy::new(
            |event| {
                report.get_or_insert(event);
            },
            recover,
        ),
        check_scope,
    );
    assert!(report.is_none(), "unexpected reclamation: {report:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
