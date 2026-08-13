use std::{
    cell::{Cell, RefCell},
    pin::{Pin, pin},
    rc::Rc,
    task::Poll,
};

use dope::core::driver::{
    route::{Epoch, KeyTag, SlotIndex},
    schedule::{self, ready::completion},
};
use dope_fiber::{
    abi::{
        Fiber, Join, Ready,
        batch::{Batch, Domain},
        future::Lazy,
        race::{Either, Race},
    },
    context::{Context, PollCall, RootWaker},
    task::{Group, Scheduler},
    wait::{Slot, Waiter},
};
use dope_test::{
    checks::{TrackingAlloc, panics::CountDrop},
    scenario::rt::{Runtime, Tokens},
};
use o3::collections::slab::Capacity;

struct PendingCount(Rc<Cell<usize>>);

impl<'d> Fiber<'d> for PendingCount {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (self_, _) = call.into_parts();
        self_.0.set(self_.0.get() + 1);
        Poll::Pending
    }
}

struct Probe {
    polls: Rc<Cell<usize>>,
    ready: bool,
}

struct StateProbe {
    polls: Rc<Cell<usize>>,
    ready: Rc<Cell<bool>>,
    output: usize,
}

impl<'d> Fiber<'d> for StateProbe {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<usize> {
        let (self_, _) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        if self_.ready.get() {
            Poll::Ready(self_.output)
        } else {
            Poll::Pending
        }
    }
}

struct WakeProbe<'d> {
    polls: Rc<Cell<usize>>,
    wake: Rc<RefCell<Option<completion::Waker<'d>>>>,
}

impl<'d> Fiber<'d> for WakeProbe<'d> {
    type Output = ();

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        let (self_, context) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        self_
            .wake
            .replace(Some(context.as_ref().completion_waker()));
        Poll::Pending
    }
}

struct CaptureReady<'d> {
    wake: Rc<RefCell<Option<completion::Waker<'d>>>>,
    drops: Rc<Cell<usize>>,
    polls: Rc<Cell<usize>>,
    ready: Rc<Cell<bool>>,
    output: u8,
}

impl Drop for CaptureReady<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl<'d> Fiber<'d> for CaptureReady<'d> {
    type Output = u8;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<u8> {
        let (self_, context) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        self_
            .wake
            .replace(Some(context.as_ref().completion_waker()));
        if self_.ready.get() {
            Poll::Ready(self_.output)
        } else {
            Poll::Pending
        }
    }
}

impl<'d> Fiber<'d> for Probe {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<usize> {
        let (self_, _) = call.into_parts();
        self_.polls.set(self_.polls.get() + 1);
        if self_.ready {
            Poll::Ready(1)
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WideTarget(u128);

#[test]
fn bounded_capacity_rejects_excess_input() {
    Runtime::throughput().with_retained_turn(|_, driver| {
        let reference = driver.driver_ref();
        let root = reference
            .ready()
            .make_ready_slot(
                reference
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let mut domain =
            Domain::<2>::try_new(RootWaker::from(root.target())).expect("batch domain");
        let polls = CountDrop::counter();
        let mut batch =
            Batch::<Probe, usize, 2>::try_empty(&mut domain).expect("batch queue allocation");
        assert!(
            batch
                .try_push(Probe {
                    polls: Rc::clone(&polls),
                    ready: true,
                })
                .is_ok()
        );
        assert!(
            batch
                .try_push(Probe {
                    polls: Rc::clone(&polls),
                    ready: true,
                })
                .is_ok()
        );
        assert!(batch.try_push(Probe { polls, ready: true }).is_err());
    });
}

#[test]
fn batch_without_a_child_wake_returns_pending() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let root = reference
            .ready()
            .make_ready_slot(
                reference
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let target = root.target();
        let polls = Rc::new(Cell::new(0));
        let mut domain = Domain::<1>::try_new(RootWaker::from(target)).expect("batch domain");
        let mut batch = pin!(
            Batch::<_, (), 1>::try_from_array(&mut domain, [PendingCount(Rc::clone(&polls))],)
                .expect("batch queue allocation")
        );
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));

        assert!(
            context
                .as_mut()
                .try_poll(batch.as_mut())
                .expect("batch poll credit")
                .is_pending()
        );
        assert_eq!(polls.get(), 1);
    });
}

#[test]
fn batch_construction_has_an_explicit_allocation_ceiling() {
    let (warm_allocations, _) = TrackingAlloc::<0>::during(|| ());
    assert_eq!(warm_allocations, 0);
    assert_eq!(
        core::mem::size_of::<Domain<'static, 1>>(),
        3 * core::mem::size_of::<usize>()
    );
    assert_eq!(
        core::mem::size_of::<Domain<'static, 4096>>(),
        core::mem::size_of::<Domain<'static, 1>>()
    );

    Runtime::throughput().with_retained_turn(|_, driver| {
        let reference = driver.driver_ref();
        let root = reference
            .ready()
            .make_ready_slot(
                reference
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let parent = RootWaker::from(root.target());

        let (inline_domain, (allocations, bytes)) = TrackingAlloc::<0>::measure(|| {
            Domain::<8>::try_new(parent).expect("inline batch domain")
        });
        assert_eq!((allocations, bytes), (0, 0));
        let mut inline_domain = inline_domain;
        let (inline, (allocations, _)) = TrackingAlloc::<0>::measure(|| {
            Batch::<Ready<()>, (), 8>::try_from_array(
                &mut inline_domain,
                core::array::from_fn(|_| Ready::new(())),
            )
            .expect("inline batch queue allocation")
        });
        assert_eq!(allocations, 0);
        drop(inline);

        let mut heap_domain = Domain::<40>::try_new(parent).expect("heap batch domain");
        let (heap, (allocations, _)) = TrackingAlloc::<0>::measure(|| {
            Batch::<Ready<()>, (), 40>::try_empty(&mut heap_domain)
                .expect("heap batch queue allocation")
        });
        assert_eq!(allocations, 1);
        drop(heap);

        let mut largest_domain = Domain::<512>::try_new(parent).expect("largest batch domain");
        let (largest_consumer, (allocations, _)) = TrackingAlloc::<0>::measure(|| {
            Batch::<Ready<()>, (), 512>::try_empty(&mut largest_domain)
                .expect("largest batch queue allocation")
        });
        assert_eq!(allocations, 1);
        drop(largest_consumer);
    });
}

#[test]
fn group_hot_insert_drive_complete_and_reuse_allocate_nothing() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let target = root.target();
        let parent = RootWaker::from(target);
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));
        let (heap, (allocations, _)) = TrackingAlloc::<0>::measure(|| {
            Group::<Ready<usize>, 40>::try_new(parent).expect("heap group allocation")
        });
        assert_eq!(allocations, 1);
        drop(heap);
        let (group, (allocations, _)) = TrackingAlloc::<0>::measure(|| {
            Group::<Ready<usize>, 8>::try_new(parent).expect("inline group allocation")
        });
        assert_eq!(allocations, 0);
        let mut group = pin!(group);
        let mut sum = 0;

        let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
            for value in 0..8 {
                assert!(group.as_mut().try_push(Ready::new(value)).is_ok());
            }
            assert_eq!(
                group
                    .as_mut()
                    .drive_ready(context.as_mut(), |value| sum += value),
                8
            );
            assert!(group.is_empty());
            assert!(group.as_mut().try_push(Ready::new(8)).is_ok());
            assert_eq!(
                group
                    .as_mut()
                    .drive_ready(context.as_mut(), |value| sum += value),
                1
            );
        });
        assert_eq!((allocations, bytes), (0, 0));
        assert_eq!(sum, (0..=8).sum());
    });
}

#[test]
fn group_insert_activates_its_exact_parent_without_an_immediate_drive() {
    Runtime::throughput().with_retained_turn(|turn, driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let polls = Rc::new(Cell::new(0));
        let mut group = pin!(
            Group::<PendingCount, 1>::try_new(RootWaker::from(root.target()))
                .expect("group allocation")
        );

        let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
            assert!(
                group
                    .as_mut()
                    .try_push(PendingCount(Rc::clone(&polls)))
                    .is_ok()
            );
        });
        assert_eq!((allocations, bytes), (0, 0));
        assert_eq!(polls.get(), 0);

        let activated = Cell::new(None);
        assert_eq!(
            turn.drain_ready(reference, usize::MAX, |target| {
                assert!(activated.replace(Some(Tokens::parts(target))).is_none());
            }),
            1
        );
        assert_eq!(activated.get(), Some(Tokens::at(0)));
    });
}

#[test]
fn group_polls_only_exact_ready_members() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let target = root.target();
        let parent = RootWaker::from(target);
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));
        let first_polls = Rc::new(Cell::new(0));
        let second_polls = Rc::new(Cell::new(0));
        let first_wake = Rc::new(RefCell::new(None));
        let second_wake = Rc::new(RefCell::new(None));
        let mut group = pin!(Group::<WakeProbe<'_>, 2>::try_new(parent).expect("group allocation"));
        assert!(
            group
                .as_mut()
                .try_push(WakeProbe {
                    polls: Rc::clone(&first_polls),
                    wake: Rc::clone(&first_wake),
                })
                .is_ok()
        );
        assert!(
            group
                .as_mut()
                .try_push(WakeProbe {
                    polls: Rc::clone(&second_polls),
                    wake: Rc::clone(&second_wake),
                })
                .is_ok()
        );

        assert_eq!(group.as_mut().drive_ready(context.as_mut(), |_| {}), 0);
        assert_eq!((first_polls.get(), second_polls.get()), (1, 1));
        first_wake.borrow_mut().take().expect("first wake").wake();
        turn.drain_ready(reference, usize::MAX, drop);
        assert_eq!(group.as_mut().drive_ready(context.as_mut(), |_| {}), 0);
        assert_eq!((first_polls.get(), second_polls.get()), (2, 1));
    });
}

#[test]
fn group_completion_drops_before_callback_and_stale_wake_cannot_hit_reuse() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let target = root.target();
        let parent = RootWaker::from(target);
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));
        let stale = Rc::new(RefCell::new(None));
        let drops = Rc::new(Cell::new(0));
        let first_polls = Rc::new(Cell::new(0));
        let mut group =
            pin!(Group::<CaptureReady<'_>, 1>::try_new(parent).expect("group allocation"));
        assert!(
            group
                .as_mut()
                .try_push(CaptureReady {
                    wake: Rc::clone(&stale),
                    drops: Rc::clone(&drops),
                    polls: Rc::clone(&first_polls),
                    ready: Rc::new(Cell::new(true)),
                    output: 7,
                })
                .is_ok()
        );
        let mut output = None;
        assert_eq!(
            group.as_mut().drive_ready(context.as_mut(), |value| {
                assert_eq!(drops.get(), 1);
                output = Some(value);
            }),
            1
        );
        assert_eq!(output, Some(7));
        assert!(group.is_empty());
        let stale = stale.borrow_mut().take().expect("stale child wake");

        let current = Rc::new(RefCell::new(None));
        let replacement_drops = Rc::new(Cell::new(0));
        let replacement_polls = Rc::new(Cell::new(0));
        let replacement_ready = Rc::new(Cell::new(false));
        assert!(
            group
                .as_mut()
                .try_push(CaptureReady {
                    wake: Rc::clone(&current),
                    drops: Rc::clone(&replacement_drops),
                    polls: Rc::clone(&replacement_polls),
                    ready: Rc::clone(&replacement_ready),
                    output: 9,
                })
                .is_ok()
        );
        assert_eq!(group.as_mut().drive_ready(context.as_mut(), |_| {}), 0);
        assert_eq!(replacement_polls.get(), 1);
        assert_eq!(replacement_drops.get(), 0);

        stale.wake();
        turn.drain_ready(reference, usize::MAX, drop);
        assert_eq!(group.as_mut().drive_ready(context.as_mut(), |_| {}), 0);
        assert_eq!(replacement_polls.get(), 1);

        replacement_ready.set(true);
        current
            .borrow_mut()
            .take()
            .expect("replacement child wake")
            .wake();
        turn.drain_ready(reference, usize::MAX, drop);
        let mut replacement = None;
        assert_eq!(
            group
                .as_mut()
                .drive_ready(context.as_mut(), |value| replacement = Some(value)),
            1
        );
        assert_eq!(replacement, Some(9));
        assert_eq!(replacement_drops.get(), 1);
    });
}

#[test]
fn batches_share_one_absolute_application_ceiling() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        const WIDTH: usize = 32;
        const BATCHES: usize = schedule::MAX_TURN_WORK_BUDGET / (WIDTH + 1);

        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let target = root.target();
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));
        let polls = Rc::new(Cell::new(0));
        let mut domain = Domain::<WIDTH>::try_new(RootWaker::from(target)).expect("batch domain");

        let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
            for _ in 0..BATCHES {
                let fibers = core::array::from_fn(|_| Probe {
                    polls: Rc::clone(&polls),
                    ready: true,
                });
                let mut batch = pin!(
                    Batch::<_, usize, WIDTH>::try_from_array(&mut domain, fibers)
                        .expect("batch queue allocation")
                );
                assert!(
                    context
                        .as_mut()
                        .try_poll(batch.as_mut())
                        .expect("complete batch poll credit")
                        .is_ready()
                );
            }
        });
        assert_eq!((allocations, bytes), (0, 0));
        assert_eq!(polls.get(), BATCHES * WIDTH);

        let fibers = core::array::from_fn(|_| Probe {
            polls: Rc::clone(&polls),
            ready: true,
        });
        let mut blocked = pin!(
            Batch::<_, usize, WIDTH>::try_from_array(&mut domain, fibers)
                .expect("batch queue allocation")
        );
        assert!(
            context
                .as_mut()
                .try_poll(blocked.as_mut())
                .expect("final batch poll credit")
                .is_pending()
        );
        assert_eq!(polls.get() + BATCHES + 1, schedule::MAX_TURN_WORK_BUDGET);
    });
}

#[test]
fn idle_batch_domain_retargets_to_the_next_exact_root() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let first = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("first root slot");
        let second = reference
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("second root slot");
        let mut domain =
            Domain::<1>::try_new(RootWaker::from(first.target())).expect("batch domain");

        {
            let mut context = pin!(Context::from_target(
                first.target(),
                turn.application(),
                driver.reborrow(),
            ));
            let mut batch = pin!(
                Batch::<_, (), 1>::try_from_array(&mut domain, [Ready::new(())])
                    .expect("first batch")
            );
            assert!(
                context
                    .as_mut()
                    .try_poll(batch.as_mut())
                    .expect("first batch credit")
                    .is_ready()
            );
        }

        let wake = Rc::new(RefCell::new(None));
        let polls = Rc::new(Cell::new(0));
        let mut context = pin!(Context::from_target(
            second.target(),
            turn.application(),
            driver.reborrow(),
        ));
        let mut batch = pin!(
            Batch::<_, (), 1>::try_from_array(
                &mut domain,
                [WakeProbe {
                    polls: Rc::clone(&polls),
                    wake: Rc::clone(&wake),
                }],
            )
            .expect("second batch")
        );
        assert!(
            context
                .as_mut()
                .try_poll(batch.as_mut())
                .expect("second batch credit")
                .is_pending()
        );
        wake.borrow_mut().take().expect("child wake").wake();

        let mut activated = Vec::new();
        assert_eq!(
            turn.drain_ready(reference, usize::MAX, |target| {
                activated.push(Tokens::parts(target));
            }),
            1
        );
        assert_eq!(
            turn.drain_ready(reference, usize::MAX, |target| {
                activated.push(Tokens::parts(target));
            }),
            1
        );
        assert_eq!(activated, [Tokens::at(1)]);
        assert_eq!(polls.get(), 1);
    });
}

#[test]
fn generated_awaits_share_the_same_absolute_application_ceiling() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let target = root.target();
        let mut context = pin!(Context::from_target(
            target,
            turn.application(),
            driver.reborrow(),
        ));
        let completed = Rc::new(Cell::new(0));
        let observed = Rc::clone(&completed);
        let mut fiber = pin!(dope_gen::fiber!('_, crate = ::dope_fiber => async move {
            loop {
                Ready::new(()).await;
                observed.set(observed.get() + 1);
            }
        }));

        assert!(
            context
                .as_mut()
                .try_poll(fiber.as_mut())
                .expect("adapter poll credit")
                .is_pending()
        );
        assert_eq!(
            completed.get(),
            schedule::MAX_TURN_WORK_BUDGET - 1,
            "one credit enters the adapter and every successful await consumes one"
        );
        assert!(!turn.application().take());
    });
}

fn poll_twice_with_five_credits<'d, F>(
    mut fiber: Pin<&mut F>,
    turn: schedule::Turn<'_, 'd>,
    mut driver: dope::core::driver::retained::Context<'_, 'd, 'd>,
) where
    F: Fiber<'d>,
{
    for _ in 0..schedule::MAX_TURN_WORK_BUDGET - 5 {
        assert!(turn.application().take());
    }
    let reference = driver.driver_ref();
    let targets = reference.targets::<KeyTag<0>>();
    let root = reference
        .ready()
        .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
        .expect("ready slot");
    let target = root.target();
    let mut context = pin!(Context::from_target(
        target,
        turn.application(),
        driver.reborrow(),
    ));
    for _ in 0..2 {
        assert!(
            context
                .as_mut()
                .try_poll(fiber.as_mut())
                .expect("outer fiber credit")
                .is_pending()
        );
    }
    assert!(!turn.application().take());
}

#[test]
fn lazy_defers_construction_without_a_child_credit() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        for _ in 1..schedule::MAX_TURN_WORK_BUDGET {
            assert!(turn.application().take());
        }
        let reference = driver.driver_ref();
        let root = reference
            .ready()
            .make_ready_slot(
                reference
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let constructed = Rc::new(Cell::new(0));
        let observed = Rc::clone(&constructed);
        let mut lazy = pin!(Lazy::new(move || {
            observed.set(observed.get() + 1);
            Ready::new(())
        }));
        let mut context = pin!(Context::from_target(
            root.target(),
            turn.application(),
            driver.reborrow(),
        ));

        assert!(
            context
                .as_mut()
                .try_poll(lazy.as_mut())
                .expect("lazy poll credit")
                .is_pending()
        );
        assert_eq!(constructed.get(), 0);
        assert!(!turn.application().take());
    });
}

#[test]
fn race_restores_left_priority_after_a_fully_admitted_pending_pass() {
    let left = Rc::new(Cell::new(0));
    let right = Rc::new(Cell::new(0));

    Runtime::throughput().with_retained_turn(|turn, driver| {
        let mut race = pin!(Race::new(
            PendingCount(Rc::clone(&left)),
            PendingCount(Rc::clone(&right)),
        ));
        let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
            poll_twice_with_five_credits(race.as_mut(), turn.reborrow(), driver);
        });
        assert_eq!((allocations, bytes), (0, 0));
    });

    assert_eq!((left.get(), right.get()), (2, 1));
}

#[test]
fn race_resumes_a_child_skipped_by_the_turn_budget() {
    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let root = reference
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let mut driver = crate::retained_context(context);
            let left = Rc::new(Cell::new(0));
            let right = Rc::new(Cell::new(0));
            let mut race = pin!(Race::new(
                Probe {
                    polls: Rc::clone(&left),
                    ready: false,
                },
                Probe {
                    polls: Rc::clone(&right),
                    ready: true,
                },
            ));

            let turn = controller.begin(2);
            let mut context = pin!(Context::from_target(
                root.target(),
                turn.turn().application(),
                driver.reborrow(),
            ));
            assert!(
                context
                    .as_mut()
                    .try_poll(race.as_mut())
                    .expect("race poll credit")
                    .is_pending()
            );
            assert_eq!((left.get(), right.get()), (1, 0));
            drop(turn);

            let mut turn = controller.begin(2);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
            let mut context = pin!(Context::from_target(
                root.target(),
                turn.turn().application(),
                driver.reborrow(),
            ));
            assert!(matches!(
                context
                    .as_mut()
                    .try_poll(race.as_mut())
                    .expect("race poll credit"),
                Poll::Ready(Either::Right(1))
            ));
            assert_eq!((left.get(), right.get()), (1, 1));
            drop(turn);
        });
    });
}

#[test]
fn nested_race_restores_declared_priority_after_pending() {
    let recv_polls = Rc::new(Cell::new(0));
    let tick_polls = Rc::new(Cell::new(0));
    let background_polls = Rc::new(Cell::new(0));
    let recv_ready = Rc::new(Cell::new(false));

    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let root = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let mut context = pin!(Context::from_target(
            root.target(),
            turn.application(),
            driver.reborrow(),
        ));
        let mut race = pin!(Race::new(
            Race::new(
                StateProbe {
                    polls: Rc::clone(&recv_polls),
                    ready: Rc::clone(&recv_ready),
                    output: 1,
                },
                StateProbe {
                    polls: Rc::clone(&tick_polls),
                    ready: Rc::new(Cell::new(false)),
                    output: 2,
                },
            ),
            StateProbe {
                polls: Rc::clone(&background_polls),
                ready: Rc::new(Cell::new(false)),
                output: 3,
            },
        ));

        assert!(
            context
                .as_mut()
                .try_poll(race.as_mut())
                .expect("race poll credit")
                .is_pending()
        );
        assert_eq!(
            (recv_polls.get(), tick_polls.get(), background_polls.get()),
            (1, 1, 1)
        );

        recv_ready.set(true);
        assert!(matches!(
            context
                .as_mut()
                .try_poll(race.as_mut())
                .expect("race poll credit"),
            Poll::Ready(Either::Left(Either::Left(1)))
        ));
        assert_eq!(
            (recv_polls.get(), tick_polls.get(), background_polls.get()),
            (2, 1, 1)
        );
    });
}

#[test]
fn join_rotates_the_first_child_when_only_one_child_credit_remains() {
    let left = Rc::new(Cell::new(0));
    let right = Rc::new(Cell::new(0));

    Runtime::throughput().with_retained_turn(|turn, driver| {
        let mut join = pin!(Join::new(
            PendingCount(Rc::clone(&left)),
            PendingCount(Rc::clone(&right)),
        ));
        poll_twice_with_five_credits(join.as_mut(), turn.reborrow(), driver);
    });

    assert_eq!((left.get(), right.get()), (1, 2));
}

#[test]
fn one_shot_slot_unlinks_on_wake_and_waiter_drop() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        let first = driver
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let second = driver
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        let slot = Box::pin(Slot::new());
        let canceled_slot = Box::pin(Slot::new());
        let waiter = Box::pin(Waiter::new());
        let occupied = Box::pin(Waiter::new());
        let canceled = Box::pin(Waiter::new());

        assert!(slot.as_ref().try_register_completion(
            waiter.as_ref(),
            completion::Wake::from(first.target()).completion(),
        ));
        assert!(slot.as_ref().try_register_completion(
            waiter.as_ref(),
            completion::Wake::from(first.target()).completion(),
        ));
        assert!(!slot.as_ref().try_register_completion(
            occupied.as_ref(),
            completion::Wake::from(second.target()).completion(),
        ));
        slot.as_ref().wake();
        assert!(!waiter.is_registered());
        assert_eq!(Tokens::drain(&mut sess).into_vec(), [Tokens::at(0)]);

        assert!(canceled_slot.as_ref().try_register_completion(
            canceled.as_ref(),
            completion::Wake::from(second.target()).completion(),
        ));
        drop(canceled);
        assert!(canceled_slot.is_empty());
        canceled_slot.as_ref().wake();
        assert!(Tokens::drain(&mut sess).into_vec().is_empty());
    });
}

#[test]
fn task_scheduler_polls_only_coalesced_ready_members() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let parent = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(parent.target());
        let first_polls = Rc::new(Cell::new(0));
        let second_polls = Rc::new(Cell::new(0));
        let mut tasks: Scheduler<'_, PendingCount, usize> =
            Scheduler::try_with_capacity(Capacity::new(2)).expect("scheduler allocation");
        let first = tasks
            .insert(PendingCount(Rc::clone(&first_polls)), 10, parent)
            .expect("first task");
        let second = tasks
            .insert(PendingCount(Rc::clone(&second_polls)), 11, parent)
            .expect("second task");

        assert!(tasks.wake(&first));
        assert!(tasks.wake(&second));
        assert_eq!(
            tasks.drive_ready(turn.application(), &mut driver, |_, ()| {}),
            0
        );
        assert_eq!((first_polls.get(), second_polls.get()), (1, 1));
        assert!(tasks.is_idle());
        assert_eq!(tasks.len(), 2);

        assert!(tasks.wake(&first));
        assert!(tasks.wake(&first));
        assert_eq!(
            tasks.drive_ready(turn.application(), &mut driver, |_, ()| {}),
            0
        );
        assert_eq!((first_polls.get(), second_polls.get()), (2, 1));
        assert!(tasks.remove(first));
        assert!(tasks.remove(second));
        assert!(tasks.is_empty());
    });
}

#[test]
fn task_scheduler_build_allocations_are_bounded_by_bitmap_depth() {
    Runtime::throughput().with_retained_turn(|_, _driver| {
        let (warm_allocations, _) = TrackingAlloc::<0>::during(|| ());
        assert_eq!(warm_allocations, 0);

        for (capacity, expected_allocations) in [(1, 2), (1_024, 3), (4_096, 7)] {
            let mut scheduler: Option<Scheduler<'_, Ready<()>, usize>> = None;
            let (allocations, _) = TrackingAlloc::<0>::during(|| {
                scheduler = Some(
                    Scheduler::try_with_capacity(Capacity::new(capacity))
                        .expect("scheduler allocation"),
                );
            });
            assert_eq!(allocations, expected_allocations, "capacity {capacity}");
            drop(scheduler);
        }
    });
}

#[test]
fn task_scheduler_preserves_each_inserted_parent() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        let first = driver
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("first ready slot");
        let second = driver
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("second ready slot");
        let first = RootWaker::from(first.target());
        let second = RootWaker::from(second.target());
        let mut tasks: Scheduler<'_, PendingCount, usize> =
            Scheduler::try_with_capacity(Capacity::new(2)).expect("scheduler allocation");

        tasks
            .insert(PendingCount(Rc::new(Cell::new(0))), 10, first)
            .expect("first task");
        tasks
            .insert(PendingCount(Rc::new(Cell::new(0))), 11, second)
            .expect("second task");

        assert_eq!(
            Tokens::drain(&mut sess).into_vec(),
            [Tokens::at(0), Tokens::at(1)]
        );
    });
}

#[test]
fn released_root_generation_remains_a_safe_noop_parent() {
    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let root = reference
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(root.target());
            let mut driver = crate::retained_context(context);
            let polls = Rc::new(Cell::new(0));
            let mut tasks: Scheduler<'_, PendingCount, usize> =
                Scheduler::try_with_capacity(Capacity::new(1)).expect("scheduler allocation");
            let task = tasks
                .insert(PendingCount(Rc::clone(&polls)), 0, parent)
                .expect("task admission");

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(polls.get(), 1);
            drop(turn);

            drop(root);
            assert!(tasks.wake(&task));
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 0);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(polls.get(), 2);
            assert!(tasks.is_idle());
            drop(turn);
            assert!(tasks.remove(task));
        });
    });
}

#[test]
fn task_scheduler_insert_and_completion_allocate_nothing() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let parent = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(parent.target());
        let mut tasks: Scheduler<'_, Ready<()>, usize> =
            Scheduler::try_with_capacity(Capacity::new(1)).expect("scheduler allocation");

        let mut task = None;
        let (insert_allocations, insert_bytes) = TrackingAlloc::<0>::during(|| {
            task = tasks.insert(Ready::new(()), 17, parent);
        });
        assert_eq!((insert_allocations, insert_bytes), (0, 0));
        let task = task.expect("task");

        let mut target = None;
        let (drive_allocations, drive_bytes) = TrackingAlloc::<0>::during(|| {
            assert_eq!(
                tasks.drive_ready(turn.application(), &mut driver, |value, ()| {
                    target = Some(value);
                }),
                1,
            );
        });
        assert_eq!((drive_allocations, drive_bytes), (0, 0));
        assert_eq!(target, Some(17));
        assert!(!tasks.wake(&task));
        assert!(tasks.is_empty());
    });
}

#[test]
fn task_scheduler_stops_at_the_shared_application_ceiling() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let parent = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(parent.target());
        const TASKS: usize = schedule::MAX_TURN_WORK_BUDGET + 17;
        let polls = Rc::new(Cell::new(0));
        let mut tasks: Scheduler<'_, PendingCount, usize> = Scheduler::try_with_capacity(
            Capacity::new(u32::try_from(TASKS).expect("test task count fits u32")),
        )
        .expect("scheduler allocation");
        for target in 0..TASKS {
            tasks
                .insert(PendingCount(Rc::clone(&polls)), target, parent)
                .expect("task admission");
        }

        assert_eq!(
            tasks.drive_ready(turn.application(), &mut driver, |_, ()| {}),
            0
        );
        assert_eq!(polls.get(), schedule::MAX_TURN_WORK_BUDGET);
        assert!(!tasks.is_idle());
        assert_eq!(tasks.len(), TASKS);
    });
}

#[test]
fn task_scheduler_resumes_across_a_budgeted_batch_boundary_without_allocating() {
    const TASKS: usize = schedule::MAX_TURN_WORK_BUDGET + 2;

    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let root = reference
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(root.target());
            let mut driver = crate::retained_context(context);
            let polls = Rc::new(Cell::new(0));
            let mut tasks: Scheduler<'_, PendingCount, usize> = Scheduler::try_with_capacity(
                Capacity::new(u32::try_from(TASKS).expect("test task count fits u32")),
            )
            .expect("scheduler allocation");
            for target in 0..TASKS {
                tasks
                    .insert(PendingCount(Rc::clone(&polls)), target, parent)
                    .expect("task admission");
            }

            let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
                let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
                assert_eq!(
                    tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                    0
                );
                assert_eq!(polls.get(), schedule::MAX_TURN_WORK_BUDGET);
                drop(turn);

                let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
                assert_eq!(
                    tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                    0
                );
                assert_eq!(polls.get(), TASKS);
                assert!(tasks.is_idle());
                drop(turn);

                let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                assert_eq!(turn.drain_ready(usize::MAX, drop), 0);
                drop(turn);
            });
            assert_eq!((allocations, bytes), (0, 0));
            assert_eq!(tasks.len(), TASKS);
        });
    });
}

#[test]
fn task_scheduler_budget_boundary_activates_only_one_exact_parent() {
    const TASKS: usize = 40;

    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let roots = (0..TASKS)
                .map(|index| {
                    let index = u16::try_from(index).expect("test root index fits u16");
                    reference
                        .ready()
                        .make_ready_slot(
                            targets
                                .bind(SlotIndex::from(index), Epoch::INITIAL)
                                .dispatch(),
                        )
                        .expect("ready slot")
                })
                .collect::<Vec<_>>();
            let mut driver = crate::retained_context(context);
            let polls = Rc::new(Cell::new(0));
            let mut tasks: Scheduler<'_, PendingCount, usize> =
                Scheduler::try_with_capacity(Capacity::new(TASKS as u32))
                    .expect("scheduler allocation");
            for (target, root) in roots.iter().enumerate() {
                tasks
                    .insert(
                        PendingCount(Rc::clone(&polls)),
                        target,
                        RootWaker::from(root.target()),
                    )
                    .expect("task admission");
            }

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), TASKS);
            drop(turn);

            let (allocations, bytes) = TrackingAlloc::<0>::during(|| {
                let turn = controller.begin(1);
                assert_eq!(
                    tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                    0
                );
                assert_eq!(polls.get(), 1);
                drop(turn);

                let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
                let activated = Cell::new(None);
                assert_eq!(
                    turn.drain_ready(usize::MAX, |target| {
                        assert!(activated.replace(Some(Tokens::parts(target))).is_none());
                    }),
                    1
                );
                assert_eq!(activated.get(), Some(Tokens::at(1)));
                drop(turn);
            });
            assert_eq!((allocations, bytes), (0, 0));
        });
    });
}

#[test]
fn removing_a_nonready_task_does_not_split_the_current_ready_batch() {
    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let root = reference
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            let parent = RootWaker::from(root.target());
            let mut driver = crate::retained_context(context);
            let first_polls = Rc::new(Cell::new(0));
            let removed_polls = Rc::new(Cell::new(0));
            let last_polls = Rc::new(Cell::new(0));
            let mut tasks: Scheduler<'_, PendingCount, usize> =
                Scheduler::try_with_capacity(Capacity::new(3)).expect("scheduler allocation");
            let first = tasks
                .insert(PendingCount(Rc::clone(&first_polls)), 0, parent)
                .expect("first task");
            let removed = tasks
                .insert(PendingCount(Rc::clone(&removed_polls)), 1, parent)
                .expect("removed task");
            let last = tasks
                .insert(PendingCount(Rc::clone(&last_polls)), 2, parent)
                .expect("last task");

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(
                (first_polls.get(), removed_polls.get(), last_polls.get()),
                (1, 1, 1)
            );
            assert!(tasks.is_idle());
            drop(turn);

            assert!(tasks.wake(&first));
            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
            assert!(tasks.remove(removed));
            assert!(tasks.wake(&last));
            assert_eq!(turn.drain_ready(usize::MAX, drop), 1);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(
                (first_polls.get(), removed_polls.get(), last_polls.get()),
                (2, 1, 2)
            );
            assert!(tasks.is_idle());
            drop(turn);

            assert!(tasks.remove(first));
            assert!(tasks.remove(last));
        });
    });
}

#[test]
fn removing_a_continuation_hands_the_ready_batch_to_a_live_parent() {
    const TASKS: usize = schedule::MAX_TURN_WORK_BUDGET + 2;

    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, context, mut controller| {
            let reference = context.driver_ref();
            let targets = reference.targets::<KeyTag<0>>();
            let closing = reference
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("closing ready slot");
            let live = reference
                .ready()
                .make_ready_slot(
                    targets
                        .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("live ready slot");
            let closing_parent = RootWaker::from(closing.target());
            let live_parent = RootWaker::from(live.target());
            let mut driver = crate::retained_context(context);
            let polls = Rc::new(Cell::new(0));
            let mut tasks: Scheduler<'_, PendingCount, usize> = Scheduler::try_with_capacity(
                Capacity::new(u32::try_from(TASKS).expect("test task count fits u32")),
            )
            .expect("scheduler allocation");
            let mut closing_tasks = Vec::with_capacity(TASKS - 1);
            for target in 0..TASKS - 1 {
                closing_tasks.push(
                    tasks
                        .insert(PendingCount(Rc::clone(&polls)), target, closing_parent)
                        .expect("closing task admission"),
                );
            }
            let live_task = tasks
                .insert(PendingCount(Rc::clone(&polls)), TASKS - 1, live_parent)
                .expect("live task admission");

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            assert_eq!(turn.drain_ready(usize::MAX, drop), 2);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(polls.get(), schedule::MAX_TURN_WORK_BUDGET);
            drop(turn);

            for task in closing_tasks {
                assert!(tasks.remove(task));
            }
            drop(closing);

            let mut turn = controller.begin(schedule::MAX_TURN_WORK_BUDGET);
            let mut activated = Vec::new();
            turn.drain_ready(usize::MAX, |target| {
                activated.push(Tokens::parts(target));
            });
            assert_eq!(activated, [Tokens::at(1)]);
            assert_eq!(
                tasks.drive_ready(turn.turn().application(), &mut driver, |_, ()| {}),
                0
            );
            assert_eq!(polls.get(), schedule::MAX_TURN_WORK_BUDGET + 1);
            assert!(tasks.is_idle());
            drop(turn);

            assert!(tasks.remove(live_task));
            assert!(tasks.is_empty());
        });
    });
}

#[test]
fn task_slab_preserves_target_type() {
    Runtime::throughput().with_retained_turn(|turn, mut driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let ready = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(ready.target());
        let target = WideTarget(u128::MAX - 17);
        let mut tasks: Scheduler<'_, Ready<()>, WideTarget> =
            Scheduler::try_with_capacity(Capacity::new(1)).expect("scheduler allocation");
        tasks.insert(Ready::new(()), target, parent).expect("task");
        let mut completed = None;
        assert_eq!(
            tasks.drive_ready(turn.application(), &mut driver, |value, ()| {
                completed = Some(value);
            }),
            1,
        );
        assert_eq!(completed, Some(target));
    });
}

#[test]
fn unbind_unlinks_queued_child() {
    Runtime::throughput().with_retained_turn(|_, driver| {
        let reference = driver.driver_ref();
        let targets = reference.targets::<KeyTag<0>>();
        let parent = reference
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        let parent = RootWaker::from(parent.target());
        let mut tasks: Scheduler<'_, PendingCount, usize> =
            Scheduler::try_with_capacity(Capacity::new(1)).expect("scheduler allocation");
        let task = tasks
            .insert(PendingCount(Rc::new(Cell::new(0))), 19, parent)
            .expect("task");
        assert!(tasks.remove(task));
        assert!(tasks.is_empty());
        assert!(tasks.is_idle());
    });
}

#[test]
fn dropped_slot_is_unlinked() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        {
            let slot = driver
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            slot.activate();
        }
        assert!(Tokens::drain(&mut sess).into_vec().is_empty());
    });
}

#[test]
fn stale_ready_key_cannot_activate_reused_slot() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        let stale = {
            let slot = driver
                .ready()
                .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("ready slot");
            slot.target()
        };
        let replacement = driver
            .ready()
            .make_ready_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            )
            .expect("ready slot");
        stale.wake();
        assert!(Tokens::drain(&mut sess).into_vec().is_empty());
        replacement.activate();
        assert_eq!(Tokens::drain(&mut sess).into_vec(), [Tokens::at(1)]);
    });
}

#[test]
fn queued_slot_coalesces_latest_target() {
    Runtime::throughput().with_session(|mut sess| {
        let driver = sess.driver_access().driver_ref();
        let targets = driver.targets::<KeyTag<0>>();
        let slot = driver
            .ready()
            .make_ready_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
            .expect("ready slot");
        slot.activate();
        slot.set_target(
            targets
                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                .dispatch(),
        );
        slot.activate();
        assert_eq!(Tokens::drain(&mut sess).into_vec(), [Tokens::at(1)]);
    });
}
