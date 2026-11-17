use std::{cell::Cell, num::NonZero, rc::Rc};

use dope::core::driver::{
    life::recovery::Policy,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::task::owner,
};
use dope_fiber::{
    abi::Ready,
    context::wake::RootWaker,
    task::storage::set::{Tasks, insert},
};

pub mod common;
use common::{
    recover,
    runtime::{self, Setup},
    schedule::tasks::fixtures::{
        Domain, StateProbe, WakeProbe,
        set::{Dropped, Owner},
    },
};

#[test]
fn completion_retains_slot_until_id_consumption() {
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
                .expect("parent");
            let parent = RootWaker::from(root.target());
            let polls = Rc::new(Cell::new(0));
            let ready = Rc::new(Cell::new(false));
            Tasks::<'_, StateProbe, (), Domain>::with(
                NonZero::MIN,
                owner::Capacity::new(NonZero::MIN),
                |mut tasks| {
                    let fiber = || StateProbe {
                        polls: Rc::clone(&polls),
                        ready: Rc::clone(&ready),
                        output: 7,
                    };
                    let id = tasks
                        .try_insert(fiber(), (), parent, Owner(0, 0))
                        .expect("insert");
                    tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, state| {
                        assert!(state.is_pending());
                    });
                    let id = tasks.take(id).err().expect("same pending id");
                    ready.set(true);
                    tasks.try_wake(&id).expect("wake");
                    tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, state| {
                        assert!(state.is_ready());
                    });
                    assert_eq!(polls.get(), 2);
                    assert!(matches!(
                        tasks.try_insert(fiber(), (), parent, Owner(0, 0)),
                        Err(insert::Error::Full)
                    ));
                    assert_eq!(tasks.take(id).ok(), Some(7));
                    let reused = tasks
                        .try_insert(fiber(), (), parent, Owner(0, 1))
                        .expect("recycled slot");
                    tasks.discard(reused);
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
fn live_done_and_forgotten_ids_drop_once() {
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
                .expect("parent");
            let parent = RootWaker::from(root.target());
            let drops = Cell::new(0);
            Tasks::<'_, Ready<Dropped<'_>>, (), Domain>::with(
                NonZero::new(2).expect("capacity"),
                owner::Capacity::new(NonZero::MIN),
                |mut tasks| {
                    let live = tasks
                        .try_insert(Ready::new(Dropped(&drops)), (), parent, Owner(0, 0))
                        .expect("live");
                    tasks.discard(live);
                    assert_eq!(drops.get(), 1);
                    let done = tasks
                        .try_insert(Ready::new(Dropped(&drops)), (), parent, Owner(0, 0))
                        .expect("done");
                    let forgotten = tasks
                        .try_insert(Ready::new(Dropped(&drops)), (), parent, Owner(0, 0))
                        .expect("forgotten output");
                    tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, _| {});
                    assert_eq!(drops.get(), 1);
                    tasks.discard(done);
                    assert_eq!(drops.get(), 2);
                    drop(forgotten);
                    let live = tasks
                        .try_insert(Ready::new(Dropped(&drops)), (), parent, Owner(0, 0))
                        .expect("forgotten live");
                    drop(live);
                },
            )
            .expect("tasks");
            assert_eq!(drops.get(), 4);
        },
        Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}

#[test]
fn recycled_slot_ignores_old_native_wake() {
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
                .expect("parent");
            let parent = RootWaker::from(root.target());
            let saved = Rc::new(Cell::new(None));
            let polls = Rc::new(Cell::new(0));
            Tasks::<'_, WakeProbe<'_>, (), Domain>::with(
                NonZero::MIN,
                owner::Capacity::new(NonZero::MIN),
                |mut tasks| {
                    let fiber = || WakeProbe {
                        polls: Rc::clone(&polls),
                        wake: Rc::clone(&saved),
                    };
                    let first = tasks
                        .try_insert(fiber(), (), parent, Owner(0, 0))
                        .expect("first");
                    tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, _| {});
                    let stale = saved.take().expect("saved wake");
                    tasks.discard(first);
                    let second = tasks
                        .try_insert(fiber(), (), parent, Owner(0, 1))
                        .expect("second");
                    tasks.poll_with(Owner(0, 1), turn.turn().app(), &mut driver, |_, _, _| {});
                    assert_eq!(polls.get(), 2);
                    stale.wake();
                    tasks.poll_with(Owner(0, 1), turn.turn().app(), &mut driver, |_, _, _| {});
                    assert_eq!(polls.get(), 2);
                    tasks.discard(second);
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}
