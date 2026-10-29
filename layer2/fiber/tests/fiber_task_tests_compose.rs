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
    abi::{
        Ready,
        race::{Either, Race},
    },
    context::Context,
};
use dope_test::{alloc, alloc::Tracker};

use crate::common::{
    runtime::{self, Setup},
    schedule::tasks::retained::Retain as _,
};
pub mod common;

use dope_fiber::abi::Join;

use crate::common::schedule::tasks::fixtures::*;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn awaits_share_ceiling() {
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
            let done = Rc::new(Cell::new(0));
            let observed = Rc::clone(&done);
            let mut fiber = pin!(dope_fiber::abi::Task::new(
                dope_fiber::fiber!('_ => async move {
                    loop {
                        async { Ready::new(()).await }.await;
                        observed.set(observed.get() + 1);
                    }
                })
            ));

            assert!(Context::with_target(
                target,
                turn.turn().app(),
                driver.reborrow(),
                |mut context| Polled::polled(context.as_mut().try_poll(fiber.as_mut()))
                    .expect("poll budget")
                    .is_pending(),
            ));
            assert_eq!(
                done.get(),
                schedule::LIMIT,
                "entry admits the first descent and subsequent awaits consume credits"
            );
            assert!(turn.turn().app().permit().is_none());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn returns_keep_admission() {
    for failed in [false, true] {
        Setup::throughput().with_turn(
            |turn, driver| {
                for _ in 1..schedule::LIMIT {
                    assert!(turn.turn().app().permit().is_some());
                }
                let core = driver.driver_ref();
                let root = core
                    .ready()
                    .make_slot(
                        core.targets::<KeyTag<0>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch(),
                    )
                    .expect("ready slot");
                let entered = Cell::new(0);
                let retained = Rc::new(());
                let mut fiber = pin!(dope_fiber::abi::Task::new(lexical_scope(
                    failed,
                    &entered,
                    Rc::clone(&retained),
                )));
                let outcome =
                    Context::with_target(root.target(), turn.turn().app(), driver, |context| {
                        context.poll(fiber.as_mut())
                    });
                assert_eq!(outcome, Poll::Ready(if failed { Err(7) } else { Ok(()) }));
                assert_eq!(entered.get(), 2);
                assert_eq!(Rc::strong_count(&retained), 1);
                assert!(turn.turn().app().permit().is_none());
            },
            recovery::Policy::new(|_| {}, recover),
            check_scope,
        );
    }
}

#[test]
fn lazy_waits_for_admission() {
    let mut reports = Vec::new();
    Setup::throughput().with_turn(
        |turn, mut driver| {
            for _ in 0..schedule::LIMIT {
                assert!(turn.turn().app().permit().is_some());
            }
            let core = driver.driver_ref();
            let root = core.ready().make_slot(
                core.targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            );
            let root = (root).expect("ready slot");
            let built = Rc::new(Cell::new(0));
            let observed = Rc::clone(&built);
            let mut lazy = pin!(dope_fiber::abi::Task::new(
                dope_fiber::fiber!('_ => async move {
                    observed.set(observed.get() + 1);
                    Ready::new(()).await
                })
            ));
            assert!(Context::with_target(
                root.target(),
                turn.turn().app(),
                driver.reborrow(),
                |mut context| matches!(
                    context.as_mut().try_poll(lazy.as_mut()),
                    dope_fiber::context::PollResult::Exhausted
                ),
            ));
            assert_eq!(built.get(), 0);
            let entered = Cell::new(0);
            let retained = Rc::new(());
            let mut lexical = pin!(dope_fiber::abi::Task::new(lexical_scope(
                false,
                &entered,
                Rc::clone(&retained),
            )));
            assert!(Context::with_target(
                root.target(),
                turn.turn().app(),
                driver.reborrow(),
                |context| matches!(
                    context.try_poll(lexical.as_mut()),
                    dope_fiber::context::PollResult::Exhausted
                ),
            ));
            assert_eq!(entered.get(), 0);
            assert_eq!(Rc::strong_count(&retained), 2);
            assert!(turn.turn().app().permit().is_none());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn owned_retains_state() {
    for complete in [false, true] {
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
                let polls = Cell::new(0);
                let drops = Cell::new(0);
                {
                    let mut task = pin!(dope_fiber::abi::Task::new(dope_fiber::abi::Owned::new(
                        OwnedOnce {
                            polls: &polls,
                            drops: &drops,
                            pending: true,
                            value: 41,
                        }
                    )));
                    Context::with_target(
                        root.target(),
                        turn.turn().app(),
                        driver.reborrow(),
                        |mut context| {
                            assert_eq!(context.as_mut().poll(task.as_mut()), Poll::Pending);
                            assert_eq!((polls.get(), drops.get()), (1, 0));
                            if complete {
                                assert_eq!(context.as_mut().poll(task.as_mut()), Poll::Ready(41));
                                assert_eq!((polls.get(), drops.get()), (2, 1));
                                assert_eq!(context.poll(task.as_mut()), Poll::Pending);
                                assert_eq!((polls.get(), drops.get()), (2, 1));
                            }
                        },
                    );
                    if !complete {
                        for _ in 1..schedule::LIMIT {
                            assert!(turn.turn().app().permit().is_some());
                        }
                        Context::with_target(
                            root.target(),
                            turn.turn().app(),
                            driver.reborrow(),
                            |context| {
                                assert!(matches!(
                                    context.try_poll(task.as_mut()),
                                    dope_fiber::context::PollResult::Exhausted
                                ));
                            },
                        );
                        assert_eq!((polls.get(), drops.get()), (1, 0));
                    }
                }
                assert_eq!(drops.get(), 1);
            },
            recovery::Policy::new(|_| {}, recover),
            check_scope,
        );
    }
}

#[test]
fn pins_preserve_admission() {
    Setup::throughput().with_turn(
        |turn, driver| {
            for _ in 1..schedule::LIMIT {
                assert!(turn.turn().app().permit().is_some());
            }
            let core = driver.driver_ref();
            let root = core
                .ready()
                .make_slot(
                    core.targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready slot");
            let mut fiber = pin!(dope_fiber::abi::Task::new(Ready::new(7)));
            let mut borrowed = pin!(dope_fiber::abi::Task::new(fiber.as_mut()));
            Context::with_target(root.target(), turn.turn().app(), driver, |mut context| {
                assert_eq!(context.as_mut().poll(borrowed.as_mut()), Poll::Ready(7));
            });
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

#[test]
fn nested_completion() {
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
            let drops = Rc::new(Cell::new(0));
            let polls = Rc::new(Cell::new(0));
            let leaf = CaptureReady {
                wake: Rc::new(Cell::new(None)),
                drops: Rc::clone(&drops),
                polls: Rc::clone(&polls),
                ready: Rc::new(Cell::new(true)),
                output: 7,
            };
            let mut fiber = pin!(dope_fiber::abi::Task::new(nested(nested(nested(nested(
                dope_fiber::fiber!('_ => async move {
                    Race::new(leaf, dope_fiber::abi::Pending::<()>::new()).await
                })
            ))))));
            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                Context::with_target(root.target(), share.work(), driver.reborrow(), |context| {
                    assert!(matches!(
                        context.poll(fiber.as_mut()),
                        Poll::Ready(Either::Left(7))
                    ));
                });
            });
            assert_eq!(drops.get(), 1);
            for _ in 1..schedule::LIMIT {
                assert!(turn.turn().app().permit().is_some());
            }
            assert!(turn.turn().app().permit().is_none());
            Context::with_target(root.target(), turn.turn().app(), driver, |context| {
                assert!(matches!(
                    context.try_poll(fiber.as_mut()),
                    dope_fiber::context::PollResult::Polled(Poll::Pending)
                ));
            });
            assert_eq!(polls.get(), 1);
            assert_eq!(drops.get(), 1);
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

#[test]
fn race_restores_left() {
    let mut reports = Vec::new();
    let left = Rc::new(Cell::new(0));
    let right = Rc::new(Cell::new(0));

    Setup::throughput().with_turn(
        |turn, driver| {
            let mut race = pin!(dope_fiber::abi::Task::new(Race::new(
                PendingCount(Rc::clone(&left)),
                PendingCount(Rc::clone(&right)),
            )));
            let (observed, activity) =
                Tracker::usage(|| poll_twice(race.as_mut(), turn.turn(), driver));
            assert_eq!(
                observed.expect("ready slot"),
                (schedule::LIMIT - 3, [Some(true); 2], true)
            );
            assert_eq!(activity, (0, 0));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);

    assert_eq!((left.get(), right.get()), (2, 1));
}

#[test]
fn race_repoll_is_inert() {
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
            let left = Rc::new(Cell::new(0));
            let right = Rc::new(Cell::new(0));
            let mut race = pin!(dope_fiber::abi::Task::new(Race::new(
                Probe {
                    polls: Rc::clone(&left),
                    ready: true,
                },
                PendingCount(Rc::clone(&right)),
            )));
            Context::with_target(
                root.target(),
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    assert!(matches!(
                        Polled::polled(context.as_mut().try_poll(race.as_mut()))
                            .expect("poll budget"),
                        Poll::Ready(Either::Left(1))
                    ));
                    assert_eq!((left.get(), right.get()), (1, 0));
                    assert!(matches!(
                        Polled::polled(context.as_mut().try_poll(race.as_mut()))
                            .expect("poll budget"),
                        Poll::Pending
                    ));
                    assert_eq!((left.get(), right.get()), (1, 0));
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn race_resumes_skipped() {
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
                let mut driver = context.retained();
                let left = Rc::new(Cell::new(0));
                let right = Rc::new(Cell::new(0));
                let mut race = pin!(dope_fiber::abi::Task::new(Race::new(
                    Probe {
                        polls: Rc::clone(&left),
                        ready: false,
                    },
                    Probe {
                        polls: Rc::clone(&right),
                        ready: true,
                    },
                )));

                {
                    let turn = control.begin();
                    assert!(turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        Context::with_target(
                            root.target(),
                            share.work(),
                            driver.reborrow(),
                            |mut context| {
                                Polled::polled(context.as_mut().try_poll(race.as_mut()))
                                    .expect("poll budget")
                                    .is_pending()
                            },
                        )
                    }));
                    assert_eq!((left.get(), right.get()), (1, 0));
                }

                {
                    let turn = control.begin();
                    assert_eq!(turn.ready_round(|_| {}), 1);
                    assert!(matches!(
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            Context::with_target(
                                root.target(),
                                share.work(),
                                driver.reborrow(),
                                |mut context| {
                                    Polled::polled(context.as_mut().try_poll(race.as_mut()))
                                        .expect("poll budget")
                                },
                            )
                        }),
                        Poll::Ready(Either::Right(1))
                    ));
                    assert_eq!((left.get(), right.get()), (1, 1));
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn nested_race_restores() {
    let mut reports = Vec::new();
    let recv = Rc::new(Cell::new(0));
    let tick = Rc::new(Cell::new(0));
    let back = Rc::new(Cell::new(0));
    let ready = Rc::new(Cell::new(false));

    Setup::throughput().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("ready slot");
            let mut race = pin!(dope_fiber::abi::Task::new(Race::new(
                Race::new(
                    StateProbe {
                        polls: Rc::clone(&recv),
                        ready: Rc::clone(&ready),
                        output: 1,
                    },
                    StateProbe {
                        polls: Rc::clone(&tick),
                        ready: Rc::new(Cell::new(false)),
                        output: 2,
                    },
                ),
                StateProbe {
                    polls: Rc::clone(&back),
                    ready: Rc::new(Cell::new(false)),
                    output: 3,
                },
            )));

            Context::with_target(
                root.target(),
                turn.turn().app(),
                driver.reborrow(),
                |mut context| {
                    assert!(
                        Polled::polled(context.as_mut().try_poll(race.as_mut()))
                            .expect("poll budget")
                            .is_pending()
                    );
                    assert_eq!((recv.get(), tick.get(), back.get()), (1, 1, 1));

                    ready.set(true);
                    assert!(matches!(
                        Polled::polled(context.as_mut().try_poll(race.as_mut()))
                            .expect("poll budget"),
                        Poll::Ready(Either::Left(Either::Left(1)))
                    ));
                    assert_eq!((recv.get(), tick.get(), back.get()), (2, 1, 1));
                },
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);
}

#[test]
fn join_rotates_first() {
    let mut reports = Vec::new();
    let left = Rc::new(Cell::new(0));
    let right = Rc::new(Cell::new(0));

    Setup::throughput().with_turn(
        |turn, driver| {
            let mut join = pin!(dope_fiber::abi::Task::new(Join::new(
                PendingCount(Rc::clone(&left)),
                PendingCount(Rc::clone(&right)),
            )));
            assert_eq!(
                poll_twice(join.as_mut(), turn.turn(), driver).expect("ready slot"),
                (schedule::LIMIT - 3, [Some(true); 2], true)
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    check_reports(&reports);

    assert_eq!((left.get(), right.get()), (1, 2));
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
