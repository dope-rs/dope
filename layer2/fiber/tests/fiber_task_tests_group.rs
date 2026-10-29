use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc};

use dope::core::{
    driver::{
        life::recovery,
        route::{Epoch, KeyTag, indices::SlotIndex},
    },
    io::Reclaimed,
};
use dope_fiber::{
    abi::{Pending, Ready},
    context::{Context, wake::RootWaker},
};
use dope_test::{alloc, alloc::Tracker};
use o3::depot::array::Len;

use crate::common::runtime::{self, Setup};
pub mod common;

use core::array::from_fn;

use dope_fiber::task::group::{Drive, Group, Vacant};

use crate::common::schedule::tasks::fixtures::*;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn layout_is_compact() {
    use std::mem;

    assert_eq!(
        mem::size_of::<Vacant<'static, 'static, Ready<()>, 1>>(),
        2 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Group<'static, Ready<()>, 1>>() < 296);
    assert!(mem::size_of::<Group<'static, Ready<()>, 8>>() < 632);
    assert_eq!(
        std::alloc::Layout::new::<Group<'static, Pending<()>, 8>>(),
        std::alloc::Layout::new::<Group<'static, Pending<[u128; 64]>, 8>>()
    );
}

#[test]
fn self_wake_waits_for_turn() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let target = root.target();
            let polls = Rc::new(Cell::new(0));
            let group = Group::<SelfWake, 1>::try_new(RootWaker::from(target));
            let group = (group).expect("group domain");
            let mut group = pin!(group);
            let vacant = group.as_mut().vacant().expect("group vacancy");
            vacant.insert(SelfWake(Rc::clone(&polls)));

            Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!(polls.get(), 1);
                    turn.ready_round(|_| {});
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!(polls.get(), 2);
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn insert_wakes_parent() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let polls = Rc::new(Cell::new(0));
            let group = Group::<PendingCount, 1>::try_new(RootWaker::from(root.target()));
            let group = (group).expect("group allocation");
            let mut group = pin!(group);

            let (allocs, bytes) = Tracker::during(|| {
                let vacant = group.as_mut().vacant();
                let vacant = (vacant).expect("pending group vacancy");
                vacant.insert(PendingCount(Rc::clone(&polls)));
            });
            assert_eq!((allocs, bytes), (0, 0));
            assert_eq!(polls.get(), 0);

            let woken = Cell::new(None);
            assert_eq!(
                turn.ready_round(|target| {
                    assert!(woken.replace(Some(target)).is_none());
                }),
                1
            );
            assert!(woken.get().is_some_and(|target| {
                targets
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch()
                    .matches(target)
            }));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn polls_ready_members() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let target = root.target();
            let parent = RootWaker::from(target);
            let polls1 = Rc::new(Cell::new(0));
            let polls2 = Rc::new(Cell::new(0));
            let wake1 = Rc::new(Cell::new(None));
            let wake2 = Rc::new(Cell::new(None));
            let group = Group::<WakeProbe<'_>, 2>::try_new(parent);
            let group = (group).expect("group allocation");
            let mut group = pin!(group);
            let vacant = group.as_mut().vacant();
            let vacant = (vacant).expect("first group vacancy");
            vacant.insert(WakeProbe {
                polls: Rc::clone(&polls1),
                wake: Rc::clone(&wake1),
            });
            let vacant = group.as_mut().vacant();
            let vacant = (vacant).expect("second group vacancy");
            vacant.insert(WakeProbe {
                polls: Rc::clone(&polls2),
                wake: Rc::clone(&wake2),
            });

            Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!((polls1.get(), polls2.get()), (1, 1));
                    let wake = wake1.take();
                    let wake = (wake).expect("first wake");
                    wake.wake();
                    turn.ready_round(|_| {});
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!((polls1.get(), polls2.get()), (2, 1));
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn stale_wake_misses_reuse() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let target = root.target();
            let parent = RootWaker::from(target);
            let stale = Rc::new(Cell::new(None));
            let drops = Rc::new(Cell::new(0));
            let polls1 = Rc::new(Cell::new(0));
            let group = Group::try_with(
                parent,
                [CaptureReady {
                    wake: Rc::clone(&stale),
                    drops: Rc::clone(&drops),
                    polls: Rc::clone(&polls1),
                    ready: Rc::new(Cell::new(true)),
                    output: 7,
                }],
            );
            let group = (group).expect("group allocation");
            let mut group = pin!(group);
            Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    let mut output = None;
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |value| {
                            assert_eq!(drops.get(), 1);
                            output = Some(value);
                        }) == Drive::Complete(Len::exact::<1>())
                    );
                    assert_eq!(output, Some(7));
                    assert!(group.len() == Len::ZERO);
                    let stale = stale.take();
                    let stale = (stale).expect("stale child wake");

                    let current = Rc::new(Cell::new(None));
                    let drops2 = Rc::new(Cell::new(0));
                    let polls2 = Rc::new(Cell::new(0));
                    let ready2 = Rc::new(Cell::new(false));
                    let vacant = group.as_mut().vacant();
                    let vacant = (vacant).expect("replacement group vacancy");
                    vacant.insert(CaptureReady {
                        wake: Rc::clone(&current),
                        drops: Rc::clone(&drops2),
                        polls: Rc::clone(&polls2),
                        ready: Rc::clone(&ready2),
                        output: 9,
                    });
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!(polls2.get(), 1);
                    assert_eq!(drops2.get(), 0);

                    stale.wake();
                    turn.ready_round(|_| {});
                    assert!(
                        group.as_mut().drive_ready(context.as_mut(), |_| {})
                            == Drive::Complete(Len::ZERO)
                    );
                    assert_eq!(polls2.get(), 1);

                    ready2.set(true);
                    let current = current.take();
                    let current = (current).expect("replacement child wake");
                    current.wake();
                    turn.ready_round(|_| {});
                    let mut next = None;
                    assert!(
                        group
                            .as_mut()
                            .drive_ready(context.as_mut(), |value| next = Some(value))
                            == Drive::Complete(Len::exact::<1>())
                    );
                    assert_eq!(next, Some(9));
                    assert_eq!(drops2.get(), 1);
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
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
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let target = root.target();
            let parent = RootWaker::from(target);
            let (group, (allocs, bytes)) =
                Tracker::usage(|| Group::<Ready<usize>, 16>::try_with(parent, from_fn(Ready::new)));
            assert_eq!((allocs, bytes), (0, 0));
            let group = (group).expect("inline group allocation");
            let mut group = pin!(group);
            assert!(group.len() == Len::MAX);
            assert!(group.is_ready());
            assert!(group.as_mut().vacant().is_none());
            assert_eq!(turn.ready_round(|_| {}), 1);
            let mut next = 0;

            let (allocs, bytes) = Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    Tracker::during(|| {
                        assert!(
                            group.as_mut().drive_ready(context.as_mut(), |value| {
                                assert_eq!(value, next);
                                next += 1;
                            }) == Drive::Complete(Len::MAX)
                        );
                        assert!(group.len() == Len::ZERO);
                        let vacant = group.as_mut().vacant();
                        let vacant = (vacant).expect("reused group vacancy");
                        vacant.insert(Ready::new(16));
                        assert!(
                            group.as_mut().drive_ready(context.as_mut(), |value| {
                                assert_eq!(value, next);
                                next += 1;
                            }) == Drive::Complete(Len::exact::<1>())
                        );
                    })
                },
            );
            assert_eq!((allocs, bytes), (0, 0));
            assert_eq!(next, 17);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}

fn check_reports(reports: &[Reclaimed]) {
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
