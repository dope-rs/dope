use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc};

use dope::core::{
    driver::{
        life::recovery::{self, Policy},
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule,
    },
    io::Reclaimed,
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::{
        scope::Scope,
        set::{Access as _, Length, Set, Work},
    },
};
use dope_test::{alloc, alloc::Tracker, harness::observe::Trace};
use o3::depot::capacity::Count;

use crate::common::{
    runtime::{self, Setup},
    schedule::tasks::retained::Retain as _,
};
pub mod common;

use crate::common::schedule::tasks::fixtures::*;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

#[test]
fn resumes_after_budget() {
    const TASKS: usize = schedule::LIMIT + 2;
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let root = core
                    .ready()
                    .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
                let root = (root).expect("ready slot");
                let parent = RootWaker::from(root.target());
                let mut driver = context.retained();
                let polls = Rc::new(Cell::new(0));
                let count = (u32::try_from(TASKS)).expect("task count fits u32");
                let tasks = Set::try_new(Count::new(count));
                let tasks: Set<'_, PendingCount, usize, Domain> =
                    (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    for target in 0..TASKS {
                        assert!(
                            tasks
                                .try_insert(PendingCount(Rc::clone(&polls)), target, parent)
                                .is_ok(),
                            "task admission"
                        );
                    }

                    let (allocs, bytes) = Tracker::during(|| {
                        {
                            let turn = control.begin();
                            assert_eq!(turn.ready_round(|_| {}), 1);
                            assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                            assert_eq!(polls.get(), schedule::LIMIT);
                            assert!(tasks.work() == Work::Pending);
                        }

                        {
                            let turn = control.begin();
                            assert_eq!(turn.ready_round(|_| {}), 1);
                            assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                            assert_eq!(polls.get(), TASKS);
                            assert!(tasks.work() == Work::Idle);
                        }

                        {
                            let turn = control.begin();
                            assert_eq!(turn.ready_round(|_| {}), 0);
                        }
                    });
                    assert_eq!((allocs, bytes), (0, 0));
                    assert_eq!(tasks.len().get(), TASKS);
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn budget_wakes_one_parent() {
    const TASKS: usize = 40;
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let mut roots = Vec::with_capacity(TASKS);
                for raw in 0..TASKS {
                    let index = (u16::try_from(raw)).expect("root index fits u16");
                    let root = core.ready().make_slot(
                        targets
                            .bind(SlotIndex::from(index), Epoch::INITIAL)
                            .dispatch(),
                    );
                    let root = (root).expect("ready slot");
                    roots.push(root);
                }
                let mut driver = context.retained();
                let polls = Rc::new(Cell::new(0));
                let count = (u32::try_from(TASKS)).expect("task count fits u32");
                let tasks = Set::try_new(Count::new(count));
                let tasks: Set<'_, PendingCount, usize, Domain> =
                    (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    for (target, root) in roots.iter().enumerate() {
                        assert!(
                            tasks
                                .try_insert(
                                    PendingCount(Rc::clone(&polls)),
                                    target,
                                    RootWaker::from(root.target()),
                                )
                                .is_ok(),
                            "task admission"
                        );
                    }

                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), TASKS);
                    }

                    let (allocs, bytes) = Tracker::during(|| {
                        {
                            let turn = control.begin();
                            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                assert_eq!(tasks.poll_ready(share.work(), &mut driver).get(), 0);
                            });
                            assert_eq!(polls.get(), 1);
                        }

                        {
                            let turn = control.begin();
                            let woken = Cell::new(None);
                            assert_eq!(
                                turn.ready_round(|target| {
                                    assert!(woken.replace(Some(target)).is_none());
                                }),
                                1
                            );
                            assert!(woken.get().is_some_and(|target| {
                                targets
                                    .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                                    .dispatch()
                                    .matches(target)
                            }));
                        }
                    });
                    assert_eq!((allocs, bytes), (0, 0));
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn remove_keeps_batch() {
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let root = core
                    .ready()
                    .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
                let root = (root).expect("ready slot");
                let parent = RootWaker::from(root.target());
                let mut driver = context.retained();
                let polls1 = Rc::new(Cell::new(0));
                let polls2 = Rc::new(Cell::new(0));
                let last_n = Rc::new(Cell::new(0));
                let tasks = Set::try_new(Count::new(3));
                let tasks: Set<'_, PendingCount, usize, Domain> =
                    (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    let first = tasks.try_insert(PendingCount(Rc::clone(&polls1)), 0, parent);
                    let first = (first).expect("first task");
                    let removed = tasks.try_insert(PendingCount(Rc::clone(&polls2)), 1, parent);
                    let removed = (removed).expect("removed task");
                    let last = tasks.try_insert(PendingCount(Rc::clone(&last_n)), 2, parent);
                    let last = (last).expect("last task");

                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 1);
                        assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                        assert_eq!((polls1.get(), polls2.get(), last_n.get()), (1, 1, 1));
                        assert!(tasks.work() == Work::Idle);
                    }

                    assert!(tasks.try_wake(&first).is_ok());
                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 1);
                        assert!(tasks.remove(removed).is_ok());
                        assert!(tasks.try_wake(&last).is_ok());
                        assert_eq!(turn.ready_round(|_| {}), 1);
                        assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                        assert_eq!((polls1.get(), polls2.get(), last_n.get()), (2, 1, 2));
                        assert!(tasks.work() == Work::Idle);
                    }

                    assert!(tasks.remove(first).is_ok());
                    assert!(tasks.remove(last).is_ok());
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn remove_hands_off_batch() {
    const TASKS: usize = schedule::LIMIT + 2;
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let mut driver = context.retained();
                let polls = Rc::new(Cell::new(0));
                let count = (u32::try_from(TASKS)).expect("task count fits u32");
                let tasks = Set::try_new(Count::new(count));
                let tasks: Set<'_, PendingCount, usize, Domain> =
                    (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    let new_root;
                    let live = {
                        let old_root = core
                            .ready()
                            .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
                        let old_root = (old_root).expect("closing ready slot");
                        let root = core.ready().make_slot(
                            targets
                                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                                .dispatch(),
                        );
                        new_root = (root).expect("live ready slot");
                        let old_wake = RootWaker::from(old_root.target());
                        let new_wake = RootWaker::from(new_root.target());
                        let mut old_set = Vec::with_capacity(TASKS - 1);
                        for target in 0..TASKS - 1 {
                            let task =
                                tasks.try_insert(PendingCount(Rc::clone(&polls)), target, old_wake);
                            let task = (task).expect("closing task admission");
                            old_set.push(task);
                        }
                        let live =
                            tasks.try_insert(PendingCount(Rc::clone(&polls)), TASKS - 1, new_wake);
                        let live = (live).expect("live task admission");

                        {
                            let turn = control.begin();
                            assert_eq!(turn.ready_round(|_| {}), 2);
                            assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                            assert_eq!(polls.get(), schedule::LIMIT);
                        }

                        for task in old_set {
                            assert!(tasks.remove(task).is_ok());
                        }
                        live
                    };

                    {
                        let turn = control.begin();
                        let woken = Trace::<_, 1>::new();
                        turn.ready_round(|target| {
                            woken.record(target);
                        });
                        assert!(
                            woken
                                .matches(&[targets
                                    .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                                    .dispatch()])
                                .expect("wake observations")
                        );
                        assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                        assert_eq!(polls.get(), schedule::LIMIT + 1);
                        assert!(tasks.work() == Work::Idle);
                    }

                    assert!(tasks.remove(live).is_ok());
                    assert!(tasks.len() == Length::ZERO);
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

fn check_reports(reports: &[Reclaimed]) {
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
