use std::{cell::Cell, convert::Infallible, pin::pin};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule,
};
use dope_fiber::{
    abi::Ready,
    context::{Context, PollResult, wake::RootWaker},
    task::group::{Drive, Group},
};
use o3::depot::array::Len;

use crate::common::runtime::{self, Setup};
pub mod common;

#[test]
fn transfers_one_credit() {
    use std::task::Poll;

    use dope_fiber::abi::{PollFn, Task};

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
            let mut group = pin!(
                Group::try_with(
                    RootWaker::from(root.target()),
                    [Ready::new(()), Ready::new(())],
                )
                .expect("group")
            );
            let done = Cell::new(0);
            {
                let mut run = pin!(Task::new(PollFn::admitted(|admit| {
                    let (result, _) = group.as_mut().drive_admit(admit, |()| {
                        done.set(done.get() + 1);
                    });
                    assert!(result == Drive::Complete(Len::exact::<1>()));
                    Poll::<()>::Pending
                })));
                for expected in 1..=2 {
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        Context::with_target(
                            root.target(),
                            share.work(),
                            driver.reborrow(),
                            |mut context| {
                                assert!(matches!(
                                    context.as_mut().try_poll(run.as_mut()),
                                    PollResult::Polled(Poll::Pending),
                                ));
                            },
                        );
                        assert_eq!(done.get(), expected);
                        assert_eq!(share.work().remaining(), 0);
                    });
                }
            }
            assert!(!group.is_ready());
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

#[test]
fn seed_failure_drops() {
    use core::array::from_fn;

    use dope_fiber::abi::Owned;

    use crate::common::schedule::tasks::fixtures::{OwnedOnce, set::failure::Tight};

    Setup::for_profile::<Tight>().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("parent ready slot");
            let parent = RootWaker::from(root.target());
            let polls = Cell::new(0);
            let drops = Cell::new(0);
            for blocked in [false, true] {
                let _held = blocked.then(|| {
                    from_fn::<_, 2, _>(|index| {
                        core.ready()
                            .make_slot(
                                targets
                                    .bind(SlotIndex::from(index as u16 + 1), Epoch::INITIAL)
                                    .dispatch(),
                            )
                            .expect("blocking ready slot")
                    })
                });
                let failed = Group::<_, 2>::try_with(
                    parent,
                    from_fn(|_| {
                        Owned::new(OwnedOnce {
                            polls: &polls,
                            drops: &drops,
                            pending: true,
                            value: 0,
                        })
                    }),
                );
                let error = failed.err().expect("group exceeded ready capacity");
                assert!(matches!(
                    error.domain(),
                    schedule::ready::task::Error::Capacity { requested: need, available: have }
                        if (need.get(), *have) == if blocked { (1, 0) } else { (2, 1) }
                ));
                assert_eq!((polls.get(), drops.get()), (0, if blocked { 4 } else { 2 }));
            }
            let group = Group::try_with(
                parent,
                [Owned::new(OwnedOnce {
                    polls: &polls,
                    drops: &drops,
                    pending: true,
                    value: 0,
                })],
            )
            .expect("admissions restored");
            assert_eq!((polls.get(), drops.get()), (0, 4));
            drop(group);
            assert_eq!((polls.get(), drops.get()), (0, 5));
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

#[test]
fn seed_partial_drop() {
    use std::{array::from_fn, rc::Rc};

    use crate::common::schedule::tasks::fixtures::{CaptureReady, Rounds as _};

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
                .expect("parent ready slot");
            let polls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let wake = Rc::new(Cell::new(None));
            let stale = {
                let mut group = pin!(
                    Group::<_, 2>::try_with(
                        RootWaker::from(root.target()),
                        from_fn(|index| CaptureReady {
                            wake: Rc::clone(&wake),
                            drops: Rc::clone(&drops),
                            polls: Rc::clone(&polls),
                            ready: Rc::new(Cell::new(index == 0)),
                            output: index as u8,
                        }),
                    )
                    .expect("group")
                );
                let result = Context::with_target(
                    root.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |context| {
                        group.as_mut().drive_ready(context, |value| {
                            assert_eq!((value, drops.get()), (0, 1));
                        })
                    },
                );
                assert!(result == Drive::Complete(Len::exact::<1>()));
                assert!(group.len() == Len::exact::<1>());
                assert_eq!((polls.get(), drops.get()), (2, 1));
                wake.take().expect("pending wake")
            };
            assert_eq!((polls.get(), drops.get()), (2, 2));
            turn.ready_round(|_| {});
            stale.wake();
            assert_eq!(turn.ready_round(|_| {}), 0);
        },
        recovery::Policy::new(|_| {}, recover),
        check_scope,
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
