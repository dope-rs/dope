use dope::{
    core::driver::{life::recovery, schedule::ready::wake::store::Slot},
    runtime::executor::session::Completion,
};
use dope_fiber::{
    context::wake::{RootWaker, Waker},
    task::storage::{fixed::schedule, scope::Scope},
};
use dope_test::harness::scenario;
use o3::depot::capacity::Count;

use crate::common::runtime::{self, Empty, profile::Tasks};
pub mod common;
use common::recover;

use std::{cell::Cell, num, pin, rc::Rc};

use dope::core::driver::{
    route::{Epoch, FrameworkTag, KeyTag, indices::SlotIndex, kind},
    schedule::{LIMIT, ready::task::Error},
};
use dope_fiber::{
    BlockOn,
    abi::Ready,
    task::{
        batch,
        batch::{Join, Pool},
        storage::fixed::schedule::result::{Outcome, Selection},
    },
};

use crate::common::{runtime::Setup, storage::cases::*};

#[test]
#[deny(unsafe_code)]
fn preserves_batch_child() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_session(
        |mut session| {
            let count =
                (((session.with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                    app.block_on(dope_fiber::fiber!('_ => async move {
                        const N: usize = LIMIT;
                        let mut domain = Pool::<N>::acquire().await?;
                        let output = Join::<Ready<()>, N>::from_array(
                            &mut domain,
                            [const { Ready::new(()) }; N],
                        ).await?;
                        Ok::<_, batch::Error>(output.count())
                    }))
                }))
                .expect("batch crossed the application turn boundary"))
                .expect("batch crossed the application turn boundary"))
                .expect("batch crossed the application turn boundary");
            assert_eq!(count, LIMIT);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
#[deny(unsafe_code)]
fn binds_exact_waker() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_session(
        |mut sess| {
            let driver = sess.driver().driver_ref();
            let targets = driver.targets::<KeyTag<0>>();
            let first = (driver
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch()))
            .expect("ready slot");
            let second = (driver.ready().make_slot(
                targets
                    .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                    .dispatch(),
            ))
            .expect("ready slot");
            let stored = Cell::new(Some(Waker::from(RootWaker::from(first.target()))));
            let same = Waker::from(RootWaker::from(first.target()));
            let distinct = Waker::from(RootWaker::from(second.target()));

            assert!(stored.get().is_some_and(|current| current == same));
            assert!(stored.get().is_some_and(|current| current != distinct));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
#[deny(unsafe_code)]
fn admits_one_task() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_turn(
        |turn, mut driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let ready = (core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch()))
            .expect("ready slot");
            let parent = RootWaker::from(ready.target());
            for count in [0, 3, 16385] {
                let pool = schedule::Pool::<'_, Ready<()>, (), usize>::try_new(Count::new(count))
                    .expect("schedule task pool allocation");
                let mut slab = pin::pin!(pool);
                Scope::with(slab.as_mut(), |mut slab| {
                    assert_eq!(slab.try_admit((), parent).is_ok(), count != 0);
                    let admitted = count.min(3) as usize;
                    for target in 0..admitted {
                        let _id = slab
                            .try_admit((), parent)
                            .expect("fixed task slot")
                            .insert((Ready::new(()), target));
                    }
                    if count <= 3 {
                        let full = slab
                            .try_admit((), parent)
                            .err()
                            .expect("every fixed slot is occupied");
                        std::assert_matches!(
                            full,
                            Error::Capacity { requested, available: 0 }
                                if requested == num::NonZero::<usize>::MIN,
                        );
                    }
                    for expected in 0..admitted {
                        let admitted = match slab.as_mut().select(turn.turn().app()) {
                            Selection::Ready(admitted) => Some(admitted),
                            Selection::Idle | Selection::Yielded => None,
                        }
                        .expect("task must be ready");
                        assert!(matches!(
                            admitted.drive(&mut driver),
                            Outcome::Complete { target, output: () } if target == expected
                        ));
                    }
                    assert!(matches!(
                        slab.as_mut().select(turn.turn().app()),
                        Selection::Idle
                    ));
                });
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
#[deny(unsafe_code)]
fn revokes_plane_bindings() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tasks>().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let ready = (core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch()))
            .expect("ready slot");
            let parent = RootWaker::from(ready.target());

            let mut plane = pin::pin!(
                schedule::Pool::<'_, Ready<()>, (), usize>::try_new(Count::new(3))
                    .expect("task slots")
            );
            Scope::with(plane.as_mut(), |mut plane| {
                for target in 0..3 {
                    let _id = plane
                        .try_admit((), parent)
                        .expect("task reservation")
                        .insert((Ready::new(()), target));
                }
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
#[deny(unsafe_code)]
fn consumes_exact_wake() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::for_profile::<WakeProfile>().with_session(
        |mut session| {
            let wake = Slot::empty();
            let state = Rc::new(WakeState {
                allowed: Cell::new(false),
                polls: Cell::new(0),
                ticks: Cell::new(0),
                foreign: Cell::new(0),
                matched: Cell::new(true),
                polls_before_wake: Cell::new(None),
            });
            let driver = session.driver().driver_ref();
            let target = driver
                .targets::<FrameworkTag<kind::Any>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let slot = (driver.ready().make_slot(target)).expect("ready slot");
            slot.activate();
            let target = driver
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let root = (driver.ready().make_slot(target)).expect("application ready slot");
            root.activate();
            let result = session.with_quiesce(
                &mut finish,
                WakeProbe {
                    wake: &wake,
                    state: Rc::clone(&state),
                },
                scenario::TIMEOUT,
                |mut app| {
                    let output = (app.block_on(WakeFiber {
                        state: state.as_ref(),
                        wake: &wake,
                    }))
                    .expect("root wake failed");
                    assert_eq!(output, (2, 1))
                },
            );
            assert!(result.is_ok(), "application teardown");
            assert!(state.matched.get());
            assert_eq!(state.polls_before_wake.get(), Some(1));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}
