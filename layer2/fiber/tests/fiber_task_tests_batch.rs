use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc, task::Poll};

use dope::core::{
    driver::{
        life::recovery,
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule,
    },
    io::Reclaimed,
};
use dope_fiber::{
    abi::Ready,
    context::{Context, wake::RootWaker},
};
use dope_test::{alloc, alloc::Tracker, harness::observe::Trace};

use crate::common::{
    runtime::{self, Setup},
    storage::stores::DropOutput,
};
pub mod common;

use core::array::from_fn;

use dope_fiber::task::batch::{Join, Pool};

use crate::common::schedule::tasks::fixtures::*;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn indexed_construction() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = (root).expect("ready slot");
            let target = root.target();
            let pool = Pool::<8>::try_new(RootWaker::from(target));
            let mut pool = (pool).expect("indexed batch pool");
            let mut calls = 0;
            let (poll, activity) = Tracker::usage(|| {
                let mut batch = pin!(dope_fiber::abi::Task::new(Join::from_fn(
                    &mut pool,
                    |index| {
                        assert_eq!(index.get(), calls);
                        calls += 1;
                        Ready::new(index.get())
                    }
                )));
                Context::with_target(
                    target,
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut context| {
                        Polled::polled(context.as_mut().try_poll(batch.as_mut()))
                            .expect("poll budget")
                    },
                )
            });
            assert_eq!(calls, 8);
            assert_eq!(activity, (0, 0));
            let outputs = match poll {
                Poll::Ready(outputs) => Some(outputs),
                _ => None,
            }
            .expect("ready batch remained pending");
            let outputs = (outputs).expect("indexed batch");
            assert!(outputs.eq(0..8));

            let drops = Rc::new(Cell::new(0));
            let input: [_; 8] = from_fn(|index| {
                (
                    index,
                    DropOutput {
                        drops: Rc::clone(&drops),
                    },
                )
            });
            let mut calls = 0;
            let batch = Join::from_map(&mut pool, input, |(index, owner)| {
                assert_eq!(index, calls);
                calls += 1;
                Ready::new(owner)
            });
            assert_eq!(calls, 8);
            assert_eq!(drops.get(), 0);
            drop(batch);
            assert_eq!(drops.get(), 8);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn construction_has_ceiling() {
    let mut reports = Vec::new();

    Setup::throughput().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = (root).expect("ready slot");
            let parent = RootWaker::from(root.target());

            let (inline, (allocs, bytes)) = Tracker::usage(|| Pool::<8>::try_new(parent));
            assert_eq!((allocs, bytes), (0, 0));
            let mut inline = (inline).expect("inline batch pool");
            let ((), (allocs, _)) = Tracker::usage(|| {
                let mut batch =
                    Join::<Ready<()>, 8>::from_array(&mut inline, from_fn(|_| Ready::new(())));
                assert!(batch.try_push(Ready::new(())).is_err());
            });
            assert_eq!(allocs, 0);

            let (medium, (allocs, _)) = Tracker::usage(|| Pool::<40>::try_new(parent));
            assert_eq!(allocs, 0);
            let mut medium = (medium).expect("medium batch pool");
            let ((), (allocs, _)) = Tracker::usage(|| {
                let mut batch = Join::<Ready<()>, 40>::new(&mut medium);
                for _ in 0..40 {
                    assert!(batch.try_push(Ready::new(())).is_ok());
                }
                assert!(batch.try_push(Ready::new(())).is_err());
            });
            assert_eq!(allocs, 0);

            let (large, (allocs, _)) = Tracker::usage(|| Pool::<512>::try_new(parent));
            assert_eq!(allocs, 0);
            let mut large = (large).expect("largest batch pool");
            let ((), (allocs, _)) = Tracker::usage(|| {
                let mut batch = Join::<Ready<()>, 512>::new(&mut large);
                assert!(batch.try_push(Ready::new(())).is_ok());
            });
            assert_eq!(allocs, 0);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn batches_share_ceiling() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            const WIDTH: usize = 32;
            const BATCHES: usize = schedule::LIMIT / WIDTH;

            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let target = root.target();
            let polls = Rc::new(Cell::new(0));
            let domain = Pool::<WIDTH>::try_new(RootWaker::from(target));
            let mut domain = (domain).expect("batch pool");

            Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    let (allocs, bytes) = Tracker::during(|| {
                        for _ in 0..BATCHES {
                            let fibers = from_fn(|index| Probe {
                                polls: Rc::clone(&polls),
                                ready: [true; WIDTH][index],
                            });
                            let mut batch = pin!(dope_fiber::abi::Task::new(
                                Join::<_, WIDTH>::from_array(&mut domain, fibers)
                            ));
                            assert!(
                                Polled::polled(context.as_mut().try_poll(batch.as_mut()))
                                    .expect("poll budget")
                                    .is_ready()
                            );
                        }
                    });
                    assert_eq!((allocs, bytes), (0, 0));
                    assert_eq!(polls.get(), BATCHES * WIDTH);

                    let fibers = from_fn(|index| Probe {
                        polls: Rc::clone(&polls),
                        ready: [true; WIDTH][index],
                    });
                    let mut blocked = pin!(dope_fiber::abi::Task::new(
                        Join::<_, WIDTH>::from_array(&mut domain, fibers)
                    ));
                    assert!(matches!(
                        context.as_mut().try_poll(blocked.as_mut()),
                        dope_fiber::context::PollResult::Exhausted
                    ));
                    assert_eq!(polls.get(), schedule::LIMIT);
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn idle_retargets_root() {
    let mut reports = Vec::new();

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let first = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let first = (first).expect("first root slot");
            let second = core.ready().make_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            );
            let second = (second).expect("second root slot");
            let domain = Pool::<1>::try_new(RootWaker::from(first.target()));
            let mut domain = (domain).expect("batch pool");

            Context::with_target(
                first.target(),
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    let mut batch = pin!(dope_fiber::abi::Task::new(Join::<_, 1>::from_array(
                        &mut domain,
                        [Ready::new(())]
                    )));
                    assert!(
                        Polled::polled(context.as_mut().try_poll(batch.as_mut()))
                            .expect("poll budget")
                            .is_ready()
                    );
                },
            );

            let wake = Rc::new(Cell::new(None));
            let polls = Rc::new(Cell::new(0));
            let mut batch = pin!(dope_fiber::abi::Task::new(Join::<_, 1>::from_array(
                &mut domain,
                [WakeProbe {
                    polls: Rc::clone(&polls),
                    wake: Rc::clone(&wake),
                }],
            )));
            assert!(Context::with_target(
                second.target(),
                turn.turn().app(),
                driver.reborrow(),
                |mut context| Polled::polled(context.as_mut().try_poll(batch.as_mut()))
                    .expect("poll budget")
                    .is_pending(),
            ));
            let wake = wake.take();
            let wake = (wake).expect("child wake");
            wake.wake();

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
                        .bind(SlotIndex::fixed::<1>(), Epoch::INITIAL)
                        .dispatch()])
                    .expect("wake observations")
            );
            assert_eq!(polls.get(), 1);
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
