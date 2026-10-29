use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc};

use dope::{
    core::{
        driver::{
            life::recovery::{self, Policy},
            route::{Epoch, KeyTag, indices::SlotIndex},
        },
        io::Reclaimed,
    },
    runtime::executor::session::Completion,
};
use dope_fiber::{
    abi::Ready,
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
fn polls_coalesced() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let parent = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let parent = (parent).expect("ready slot");
            let parent = RootWaker::from(parent.target());
            let polls1 = Rc::new(Cell::new(0));
            let polls2 = Rc::new(Cell::new(0));
            let tasks: Result<Set<'_, PendingCount, usize, Domain>, _> =
                Set::try_new(Count::new(2));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                let first = tasks.try_insert(PendingCount(Rc::clone(&polls1)), 10, parent);
                let first = (first).expect("first task");
                let second = tasks.try_insert(PendingCount(Rc::clone(&polls2)), 11, parent);
                let second = (second).expect("second task");

                assert!(tasks.try_wake(&first).is_ok());
                assert!(tasks.try_wake(&second).is_ok());
                assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                assert_eq!((polls1.get(), polls2.get()), (1, 1));
                assert!(tasks.work() == Work::Idle);
                assert_eq!(tasks.len().get(), 2);

                assert!(tasks.try_wake(&first).is_ok());
                assert!(tasks.try_wake(&first).is_ok());
                assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                assert_eq!((polls1.get(), polls2.get()), (2, 1));
                assert!(tasks.remove(first).is_ok());
                assert!(tasks.remove(second).is_ok());
                assert!(tasks.len() == Length::ZERO);
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn reuse_clears_ready() {
    let mut reports = Vec::new();

    Setup::throughput().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let parent = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let parent = (parent).expect("ready slot");
            let parent = RootWaker::from(parent.target());
            let tasks: Result<Set<'_, PendingCount, (), Domain>, _> = Set::try_new(Count::new(1));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                let first = tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent);
                let first = (first).expect("first task");
                assert!(tasks.remove(first).is_ok());
                assert!(tasks.len() == Length::ZERO);
                assert!(tasks.work() == Work::Idle);

                let live = tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent);
                let live = (live).expect("reused task");
                assert!(tasks.work() == Work::Pending);
                assert!(tasks.try_wake(&live).is_ok());
                assert!(tasks.remove(live).is_ok());
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn independent_owners() {
    let mut reports = Vec::new();

    Setup::throughput().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let parent = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let parent = (parent).expect("ready slot");
            let parent = RootWaker::from(parent.target());
            let owner: Result<Set<'_, PendingCount, (), Domain>, _> = Set::try_new(Count::new(1));
            let owner = (owner).expect("owner set allocation");
            let mut owner = pin!(owner);
            Scope::with(owner.as_mut(), |mut owner| {
                let foreign: Result<Set<'_, PendingCount, (), Domain>, _> =
                    Set::try_new(Count::new(1));
                let foreign = (foreign).expect("foreign set allocation");
                let mut foreign = pin!(foreign);
                Scope::with(foreign.as_mut(), |mut foreign| {
                    let task = owner.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent);
                    let task = (task).expect("task");
                    let other = foreign.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent);
                    let other = (other).expect("foreign task");

                    assert!(owner.read(&task, |_| ()).is_some());
                    assert!(foreign.read(&other, |_| ()).is_some());
                    assert_eq!(foreign.len().get(), 1);
                    assert!(owner.remove(task).is_ok());
                    assert!(foreign.remove(other).is_ok());
                    assert!(owner.len() == Length::ZERO);
                    assert!(foreign.len() == Length::ZERO);
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn allocates_once() {
    for capacity in [1, 1_024, 4_096] {
        let (set, (allocs, _)) =
            Tracker::usage(|| Set::<'_, Ready<()>, usize, Domain>::try_new(Count::new(capacity)));
        assert_eq!(allocs, 1, "capacity {capacity}");
        let set = (set).expect("task set allocation");
        assert!(set.len() == Length::ZERO);
    }
}

#[test]
fn preserves_parents() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::throughput().with_session(
        |mut sess| {
            let driver = sess.driver().driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let first = driver
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let first = (first).expect("first ready slot");
            let second = driver.ready().make_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            );
            let second = (second).expect("second ready slot");
            let first = RootWaker::from(first.target());
            let second = RootWaker::from(second.target());
            let tasks: Result<Set<'_, PendingCount, usize, Domain>, _> =
                Set::try_new(Count::new(2));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                assert!(
                    tasks
                        .try_insert(PendingCount(Rc::new(Cell::new(0))), 10, first)
                        .is_ok(),
                    "first task"
                );
                assert!(
                    tasks
                        .try_insert(PendingCount(Rc::new(Cell::new(0))), 11, second)
                        .is_ok(),
                    "second task"
                );

                let tokens = Trace::<_, 2>::try_drain(&mut finish, &mut sess)
                    .expect("test fixture operation must succeed");
                assert!(
                    tokens
                        .matches(&[
                            targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch(),
                            targets
                                .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                                .dispatch(),
                        ])
                        .expect("wake observations")
                );
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn wake_next_keeps_owner() {
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let root1 = core
                    .ready()
                    .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
                let root1 = (root1).expect("first ready slot");
                let root2 = core.ready().make_slot(
                    targets
                        .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                        .dispatch(),
                );
                let root2 = (root2).expect("second ready slot");
                let polls = Rc::new(Cell::new(0));
                let tasks: Result<Set<'_, PendingCount, usize, Domain>, _> =
                    Set::try_new(Count::new(2));
                let tasks = (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    let first = tasks.try_insert(
                        PendingCount(Rc::clone(&polls)),
                        10,
                        RootWaker::from(root1.target()),
                    );
                    let first = (first).expect("first task");
                    let second = tasks.try_insert(
                        PendingCount(Rc::clone(&polls)),
                        11,
                        RootWaker::from(root2.target()),
                    );
                    let second = (second).expect("second task");

                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 2);
                    }

                    for _ in 0..2 {
                        let (allocs, bytes) = Tracker::during(|| tasks.wake_next());
                        assert_eq!((allocs, bytes), (0, 0));
                        assert_eq!(polls.get(), 0, "wake handoff must not poll the task");
                        assert!(
                            tasks.work() == Work::Pending,
                            "wake handoff must preserve queue ownership"
                        );

                        {
                            let turn = control.begin();
                            let woken = Trace::<_, 1>::new();
                            assert_eq!(
                                turn.ready_round(|target| {
                                    woken.record(target);
                                }),
                                1
                            );
                            assert!(
                                woken
                                    .matches(&[targets
                                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                                        .dispatch()])
                                    .expect("wake observations")
                            );
                        }
                    }

                    assert!(tasks.remove(first).is_ok());
                    assert!(tasks.remove(second).is_ok());
                    assert!(tasks.len() == Length::ZERO);
                    tasks.wake_next();
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn stale_root_is_noop() {
    let mut reports = Vec::new();

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let targets = core.targets::<KeyTag<0>>();
                let mut driver = context.retained();
                let polls = Rc::new(Cell::new(0));
                let tasks: Result<Set<'_, PendingCount, usize, Domain>, _> =
                    Set::try_new(Count::new(1));
                let tasks = (tasks).expect("task set allocation");
                let mut tasks = pin!(tasks);
                Scope::with(tasks.as_mut(), |mut tasks| {
                    let task = {
                        let root = core
                            .ready()
                            .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
                        let root = (root).expect("ready slot");
                        let parent = RootWaker::from(root.target());
                        let task = tasks.try_insert(PendingCount(Rc::clone(&polls)), 0, parent);
                        let task = (task).expect("task admission");

                        {
                            let turn = control.begin();
                            assert_eq!(turn.ready_round(|_| {}), 1);
                            assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                            assert_eq!(polls.get(), 1);
                        }
                        task
                    };

                    assert!(tasks.try_wake(&task).is_ok());
                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 0);
                        assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 0);
                        assert_eq!(polls.get(), 2);
                        assert!(tasks.work() == Work::Idle);
                    }
                    assert!(tasks.remove(task).is_ok());
                });
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn hot_path_allocates_none() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let parent = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let parent = (parent).expect("ready slot");
            let parent = RootWaker::from(parent.target());
            let tasks: Result<Set<'_, Ready<()>, usize, Domain>, _> = Set::try_new(Count::new(1));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                let mut task = None;
                let (ins_n, ins_b) = Tracker::during(|| {
                    task = Some((tasks.try_insert(Ready::new(()), 17, parent)).expect("task"));
                });
                assert_eq!((ins_n, ins_b), (0, 0));
                let task = (task).expect("task");

                let (run_n, run_b) = Tracker::during(|| {
                    assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 1);
                    let done = tasks.take_done();
                    let (target, ()) = (done).expect("completed task must retain its output");
                    assert_eq!(target, 17);
                });
                assert_eq!((run_n, run_b), (0, 0));
                assert!(tasks.try_wake(&task).is_err());
                assert!(tasks.len() == Length::ZERO);
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
