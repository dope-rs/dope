use dope_fiber::task::storage::{
    fixed::{
        direct::Plane,
        local::Removal,
        tree::{Branch, Here, Leaf, Left, Right},
    },
    scope::Scope,
};

use crate::common::runtime::profile;
pub mod common;
use common::recover;

use std::{cell::Cell, mem::size_of, pin::pin, ptr::NonNull, rc::Rc, task};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    context::Context,
    task::storage::{
        Id, fixed,
        fixed::{
            local,
            local::Store,
            quota::Limit,
            staged::{self, Completion as _},
        },
    },
};
use dope_test::alloc::Tracker;
use o3::depot::arena::slab::key;

use crate::common::{
    runtime::{self, Setup},
    storage::stores::*,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn updates_pinned_stream() {
    let observed = Rc::new(Cell::new((0, true)));
    let updated = Rc::new(Cell::new((0, true)));
    let mut stores = pin!(Plane::new(Branch::new(
        Leaf::new(fixed::Slab::<'_, Pinned, 1, Domain>::new()),
        Leaf::new(fixed::Slab::<'_, Pinned, 1, Domain>::new()),
    )));
    let allocations = Tracker::during(|| {
        Scope::with(stores.as_mut(), |mut stores| {
            let task = stores
                .as_mut()
                .route::<Left<Here>>()
                .insert(Pinned {
                    address: Cell::new(None),
                    observed: observed.clone(),
                })
                .expect("stream slot");
            let other = stores
                .as_mut()
                .route::<Right<Here>>()
                .insert(Pinned {
                    address: Cell::new(None),
                    observed: observed.clone(),
                })
                .expect("foreign stream slot");
            assert!(
                local::Stream::get_mut(stores.as_mut().route::<Right<Here>>().as_mut(), &task)
                    .is_none()
            );
            {
                let mut route = stores.as_mut().route::<Left<Here>>();
                let fiber = local::Stream::get_mut(route.as_mut(), &task).expect("live stream");
                let address = NonNull::from(fiber.as_ref().get_ref());
                let fields = fiber.project();
                fields.address.set(Some(address));
                *fields.observed = updated.clone();
            }
            assert!(stores.as_mut().route::<Left<Here>>().remove(task) == Removal::Removed);
            assert!(stores.as_mut().route::<Right<Here>>().remove(other) == Removal::Removed);
        });
    });
    assert_eq!(allocations, (0, 0));
    assert_eq!(updated.get(), (1, true));
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
                let mut slab = pin!(fixed::Slab::<'_, _, 1, Domain>::new());
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
                        |context| local::Stream::poll_next(slab.as_mut(), &task, context),
                    );
                    assert!(matches!(poll, Some(task::Poll::Pending)));
                });
            }
            assert_eq!(observed.get(), (1, true));
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
fn nominal_ids_keep_layout() {
    let key = size_of::<key::Parts>();
    assert_eq!(size_of::<Id<'static, u8>>(), 2 * key);
    assert_eq!(size_of::<Option<Id<'static, u8>>>(), 2 * key);
    assert_eq!(size_of::<Result<(), Id<'static, u8>>>(), 2 * key);
    assert_eq!(size_of::<local::Removal>(), 1);
}

#[test]
fn rejection_returns_id() {
    let owner = fixed::Slab::<'static, Empty<()>, 1, Domain>::new();
    let foreign = fixed::Slab::<'static, Empty<()>, 1, Domain>::new();
    let mut stores = pin!(Plane::new(Branch::new(
        Leaf::new(owner),
        Leaf::new(foreign)
    )));
    Scope::with(stores.as_mut(), |mut stores| {
        let id = stores
            .as_mut()
            .route::<Left<Here>>()
            .insert(Empty(()))
            .expect("owner task slot");
        let other = stores
            .as_mut()
            .route::<Right<Here>>()
            .insert(Empty(()))
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
fn deferred_completion() {
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
            let observed = Rc::new(Cell::new((0, true)));
            let drops = Rc::new(Cell::new(0));
            let slab = fixed::Slab::<'_, staged::Task<Complete, DropOutput>, 1, Domain>::new();
            let foreign =
                fixed::Slab::<'_, staged::Task<dope_fiber::abi::Pending<()>, ()>, 1, Domain>::new();
            let mut plane = pin!(Plane::bounded(
                Branch::new(Leaf::new(slab), Leaf::new(foreign)),
                Limit::new(2),
            ));
            let activity = Tracker::during(|| {
                Scope::with(plane.as_mut(), |mut plane| {
                    let foreign = plane
                        .as_mut()
                        .route::<Right<Here>>()
                        .insert(staged::Task::new(dope_fiber::abi::Pending::new()))
                        .expect("foreign task");
                    assert!(
                        staged::Store::remove_state(
                            plane.as_mut().route::<Left<Here>>().as_mut(),
                            foreign,
                        ) == staged::Removal::Absent
                    );
                    assert_eq!(plane.available().get(), 1);
                    for (removal, discard) in [
                        (None, false),
                        (None, true),
                        (Some(staged::Removal::Live), true),
                        (Some(staged::Removal::Staged), true),
                    ] {
                        observed.set((0, true));
                        drops.set(0);
                        let id = {
                            let mut route = plane.as_mut().route::<Left<Here>>();
                            let id = route
                                .insert(staged::Task::new(Complete {
                                    pinned: Pinned {
                                        address: Cell::new(None),
                                        observed: observed.clone(),
                                    },
                                    output: Some(DropOutput {
                                        drops: drops.clone(),
                                    }),
                                    pending: true,
                                }))
                                .expect("task slot");
                            assert!(matches!(
                                Context::with_target(
                                    root.target(),
                                    turn.turn().app(),
                                    driver.reborrow(),
                                    |context| staged::Store::poll_ready(
                                        route.as_mut(),
                                        &id,
                                        context
                                    ),
                                ),
                                Some(task::Poll::Pending)
                            ));
                            if removal != Some(staged::Removal::Live) {
                                let Some(task::Poll::Ready((ready, fresh))) = Context::with_target(
                                    root.target(),
                                    turn.turn().app(),
                                    driver.reborrow(),
                                    |context| {
                                        staged::Store::poll_ready(route.as_mut(), &id, context)
                                    },
                                ) else {
                                    panic!("completed task");
                                };
                                assert!(fresh);
                                assert_eq!(observed.get(), (1, true));
                                drop(|| ready.finish());
                            }
                            id
                        };
                        assert_eq!(plane.available().get(), 0);
                        assert_eq!(drops.get(), 0);
                        let mut route = plane.as_mut().route::<Left<Here>>();
                        let remaining = turn.turn().app().remaining();
                        let output = if let Some(expected) = removal {
                            assert!(staged::Store::remove_state(route.as_mut(), id) == expected);
                            None
                        } else {
                            let Some(task::Poll::Ready((ready, fresh))) = Context::with_target(
                                root.target(),
                                turn.turn().app(),
                                driver.reborrow(),
                                |context| staged::Store::poll_ready(route.as_mut(), &id, context),
                            ) else {
                                panic!("retained task");
                            };
                            assert!(!fresh);
                            if discard {
                                ready.discard();
                                None
                            } else {
                                Some(ready.finish())
                            }
                        };
                        assert_eq!(turn.turn().app().remaining(), remaining);
                        assert_eq!(observed.get(), (1, true));
                        assert!(route.capacity() == fixed::Capacity::Available);
                        assert_eq!(drops.get(), usize::from(discard));
                        drop(output);
                        assert_eq!(drops.get(), 1);
                        assert_eq!(plane.available().get(), 1);
                    }
                });
            });
            assert_eq!(activity, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(reports.is_empty());
}
