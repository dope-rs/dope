use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope::core::{
    driver::{
        life::recovery::{self, Policy},
        route::{Epoch, KeyTag, indices::SlotIndex},
    },
    io::Reclaimed,
};
use dope_fiber::{
    abi::Ready,
    context::wake::RootWaker,
    task::storage::set::{Length, Work, group::Arrival},
};
use dope_test::{alloc, alloc::Tracker};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{mem, num::NonZero};

use dope_fiber::task::storage::set::group;

use crate::common::schedule::tasks::fixtures::{set::ordered::*, *};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

#[test]
fn survives_backshift() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let parent = (core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            ))
            .expect("ready slot");
            let parent = RootWaker::from(parent.target());
            group::Tasks::<'_, '_, Ready<()>, (u8, u8), First, Domain>::with(
                NonZero::<u32>::MIN.saturating_add(2),
                owners(),
                First,
                |mut tasks| {
                    let first = tasks
                        .try_insert(Ready::new(()), (1, 10), parent)
                        .expect("first task");
                    let second =
                        (tasks.try_insert(Ready::new(()), (2, 20), parent)).expect("second task");
                    let third =
                        (tasks.try_insert(Ready::new(()), (1, 30), parent)).expect("third task");
                    assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 3);

                    let (allocs, bytes) = Tracker::during(|| {
                        assert!(matches!(
                            tasks.release_done(|task| task.1),
                            Some((Arrival::First, 10)),
                        ));
                        assert!(matches!(
                            tasks.release_done(|task| task.1),
                            Some((Arrival::First, 20)),
                        ));
                        assert!(matches!(
                            tasks.release_done(|task| task.1),
                            Some((Arrival::More, 30)),
                        ));
                    });
                    assert_eq!((allocs, bytes), (0, 0));
                    let (allocs, bytes) = Tracker::during(|| {
                        assert!(matches!(tasks.take(third), Ok(((1, 30), ()))));
                        assert_eq!(tasks.pop(Key(1)), Some(((1, 10), ())));
                        let next = tasks
                            .try_insert(Ready::new(()), (1, 40), parent)
                            .expect("reuse completed slot");
                        tasks.discard(first);
                        tasks.discard(second);
                        assert_eq!(tasks.poll_ready(turn.turn().app(), &mut driver).get(), 1);
                        assert!(matches!(
                            tasks.release_done(|task| task.1),
                            Some((Arrival::First, 40)),
                        ));
                        assert!(matches!(tasks.take(next), Ok(((1, 40), ()))));
                    });
                    assert_eq!((allocs, bytes), (0, 0));
                    assert!(tasks.len() == Length::ZERO);
                },
            )
            .expect("grouped task allocation");
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn keeps_layout_and_allocs() {
    assert_eq!(
        mem::size_of::<group::Tasks<'static, 'static, Ready<()>, (u8, u8), First, Domain>>(),
        7 * mem::size_of::<usize>(),
    );

    let (result, (allocs, _)) = Tracker::usage(|| {
        group::Tasks::<'_, '_, Ready<()>, (u8, u8), First, Domain>::with(
            NonZero::<u32>::MIN.saturating_add(4_095),
            owners(),
            First,
            |tasks| assert!(tasks.len() == Length::ZERO),
        )
    });
    assert_eq!(allocs, 3);
    result.expect("grouped task allocation");
}

#[test]
fn keeps_group_identity() {
    let mut reports = Vec::new();
    Setup::throughput().with_turn(
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
            let parent = RootWaker::from(root.target());
            let mode = Cell::new(0);
            group::Tasks::<'_, '_, Ready<u8>, ChangingKey<'_>, Changing<'_>, Domain>::with(
                NonZero::<u32>::MIN.saturating_add(3),
                owners(),
                Changing(&mode),
                |mut tasks| {
                    let first = tasks
                        .try_insert(Ready::new(10), ChangingKey::new(1, &mode), parent)
                        .expect("first member");
                    let last = tasks
                        .try_insert(Ready::new(11), ChangingKey::new(1, &mode), parent)
                        .expect("last member");
                    let second = tasks
                        .try_insert(Ready::new(20), ChangingKey::new(2, &mode), parent)
                        .expect("second group");
                    let third = tasks
                        .try_insert(Ready::new(30), ChangingKey::new(3, &mode), parent)
                        .expect("third group");
                    tasks.poll_ready(turn.turn().app(), &mut driver);
                    for _ in 0..4 {
                        tasks.release_done(|_| ()).expect("completed member");
                    }

                    mode.set(1);
                    assert!(matches!(tasks.take(second), Ok((_, 20))));
                    mode.set(0);
                    assert!(!tasks.contains(ChangingKey::new(2, &mode)));
                    mode.set(2);
                    tasks.discard(third);
                    assert!(matches!(tasks.take(first), Ok((_, 10))));

                    mode.set(0);
                    let next = tasks
                        .try_insert(Ready::new(40), ChangingKey::new(4, &mode), parent)
                        .expect("reuse first member slot");
                    tasks.poll_ready(turn.turn().app(), &mut driver);
                    tasks.release_done(|_| ()).expect("replacement completion");
                    tasks.discard(next);
                    assert!(matches!(tasks.take(last), Ok((_, 11))));
                    assert!(tasks.len() == Length::ZERO);
                },
            )
            .expect("grouped task allocation");
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn selects_owner() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let first = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("first root");
            let second = core
                .ready()
                .make_slot(
                    targets
                        .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("second root");
            let polls = [Rc::new(Cell::new(0)), Rc::new(Cell::new(0))];
            let parent = RootWaker::from(first.target());
            group::Tasks::<'_, '_, PendingCount, (u8, u8), First, Domain>::with(
                NonZero::<u32>::MIN.saturating_add(1),
                owners(),
                First,
                |mut tasks| {
                    let first = tasks
                        .try_insert(PendingCount(Rc::clone(&polls[0])), (1, 0), parent)
                        .expect("first task");
                    let second = tasks
                        .try_insert(
                            PendingCount(Rc::clone(&polls[1])),
                            (2, 0),
                            RootWaker::from(second.target()),
                        )
                        .expect("second task");
                    let mut observed = [0; 2];
                    let before = turn.turn().app().remaining();
                    let (allocs, bytes) = Tracker::during(|| {
                        assert_eq!(
                            tasks
                                .poll_with(Key(1), turn.turn().app(), &mut driver, |task, _| {
                                    assert_eq!(*task, (1, 0));
                                    observed[0] += 1;
                                })
                                .get(),
                            0
                        );
                        assert_eq!((polls[0].get(), polls[1].get()), (1, 0));
                        assert_eq!(turn.turn().app().remaining(), before - 1);
                        assert!(tasks.work() == Work::Pending);
                        assert!(tasks.work_for(Key(1)) == Work::Idle);
                        assert!(tasks.work_for(Key(2)) == Work::Pending);
                        assert_eq!(
                            tasks
                                .poll_with(Key(2), turn.turn().app(), &mut driver, |task, _| {
                                    assert_eq!(*task, (2, 0));
                                    observed[1] += 1;
                                })
                                .get(),
                            0
                        );
                        assert_eq!((polls[0].get(), polls[1].get()), (1, 1));
                        assert!(tasks.work() == Work::Idle);

                        assert!(tasks.try_wake(&second).is_ok());
                        assert!(tasks.work_for(Key(1)) == Work::Pending);
                        assert!(tasks.try_wake(&first).is_ok());
                        let before = turn.turn().app().remaining();
                        tasks.poll_with(Key(1), turn.turn().app(), &mut driver, |task, _| {
                            assert_eq!(*task, (1, 0));
                            observed[0] += 1;
                        });
                        assert_eq!((polls[0].get(), polls[1].get()), (2, 1));
                        assert_eq!(turn.turn().app().remaining(), before - 1);
                        tasks.poll_with(Key(2), turn.turn().app(), &mut driver, |task, _| {
                            assert_eq!(*task, (2, 0));
                            observed[1] += 1;
                        });
                        assert_eq!((polls[0].get(), polls[1].get()), (2, 2));
                        assert!(tasks.work() == Work::Idle);
                    });
                    assert_eq!(observed, [2, 2]);
                    assert_eq!((allocs, bytes), (0, 0));
                    assert!(tasks.try_wake(&first).is_ok());
                    tasks.discard(first);
                    let counts = Rc::new(Cell::new(0));
                    let next = tasks
                        .try_insert(PendingCount(Rc::clone(&counts)), (1, 1), parent)
                        .expect("recycled slot");
                    tasks.poll_with(Key(1), turn.turn().app(), &mut driver, |task, _| {
                        assert_eq!(*task, (1, 1));
                    });
                    assert_eq!(counts.get(), 1);
                    assert_eq!(polls[0].get(), 2);
                    tasks.discard(next);
                    tasks.discard(second);
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn rebinds_idle_owner() {
    use dope::core::driver::schedule::ready::task::owner;
    use dope_fiber::task::storage::set::insert;

    let mut reports = Vec::new();
    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("root");
            let parent = RootWaker::from(root.target());
            group::Tasks::<'_, '_, Ready<()>, EpochKey, Epochs, Domain>::with(
                NonZero::<u32>::MIN.saturating_add(1),
                owner::Capacity::new(NonZero::<u32>::MIN),
                Epochs,
                |mut tasks| {
                    let old = tasks
                        .try_insert(Ready::new(()), EpochKey(0, 0), parent)
                        .expect("old");
                    assert!(matches!(
                        tasks.try_insert(Ready::new(()), EpochKey(0, 1), parent),
                        Err(insert::Error::Owner(owner::Error::Stale {
                            current: Some(_),
                            ..
                        }))
                    ));
                    assert!(matches!(
                        tasks.try_insert(Ready::new(()), EpochKey(1, 0), parent),
                        Err(insert::Error::Owner(owner::Error::Foreign { .. }))
                    ));
                    tasks.discard(old);
                    let next = tasks
                        .try_insert(Ready::new(()), EpochKey(0, 1), parent)
                        .expect("new generation");
                    assert!(tasks.work_for(EpochKey(0, 0)) == Work::Idle);
                    assert!(tasks.work_for(EpochKey(1, 0)) == Work::Idle);
                    assert!(tasks.work_for(EpochKey(0, 1)) == Work::Pending);
                    let work = turn.turn().app();
                    let before = work.remaining();
                    assert_eq!(
                        tasks
                            .poll_with(EpochKey(0, 0), work, &mut driver, |_, _| {})
                            .get(),
                        0
                    );
                    assert_eq!(work.remaining(), before);
                    assert_eq!(
                        tasks
                            .poll_with(EpochKey(0, 1), work, &mut driver, |_, _| {})
                            .get(),
                        1
                    );
                    tasks.release_done(|_| ()).expect("completed");
                    assert!(matches!(tasks.take(next), Ok((EpochKey(0, 1), ()))));
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn failed_reserve_rebinds() {
    use dope::core::driver::schedule::ready::task::{self, owner};
    use dope_fiber::task::storage::set::insert;

    use crate::common::schedule::tasks::fixtures::set::failure::Tight;

    Setup::for_profile::<Tight>().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("root");
            let parent = RootWaker::from(root.target());
            group::Tasks::<'_, '_, Ready<()>, EpochKey, Epochs, Domain>::with(
                NonZero::<u32>::MIN,
                owner::Capacity::new(NonZero::<u32>::MIN),
                Epochs,
                |mut tasks| {
                    let first = core
                        .ready()
                        .make_slot(
                            targets
                                .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                                .dispatch(),
                        )
                        .expect("first blocker");
                    let second = core
                        .ready()
                        .make_slot(
                            targets
                                .bind(SlotIndex::fixed::<2>(), Epoch::INITIAL)
                                .dispatch(),
                        )
                        .expect("second blocker");
                    assert!(matches!(
                        tasks.try_insert(Ready::new(()), EpochKey(0, 0), parent),
                        Err(insert::Error::Admission(task::Error::Capacity {
                            available: 0,
                            ..
                        }))
                    ));
                    drop(first);
                    assert!(matches!(
                        tasks.try_insert(Ready::new(()), EpochKey(0, 1), parent),
                        Err(insert::Error::Topology(task::Error::Capacity {
                            available: 0,
                            ..
                        }))
                    ));
                    drop(second);
                    let task = tasks
                        .try_insert(Ready::new(()), EpochKey(0, 2), parent)
                        .expect("failed reservations release their owner generation");
                    tasks.discard(task);
                },
            )
            .expect("tasks");
        },
        Policy::new(|_| {}, recover),
        check_scope,
    );
}

fn check_reports(reports: &[Reclaimed]) {
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
