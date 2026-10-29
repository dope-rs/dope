use std::{cell::Cell, convert::Infallible, pin::pin, rc::Rc};

use dope::core::driver::{
    life::recovery::{self, Policy},
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::{
        scope::Scope,
        set::{Access as _, Length, Set},
    },
};
use insert::Error;
use o3::depot::capacity::Count;

use crate::common::runtime::{self, Setup};
pub mod common;

use dope::core::driver::schedule::ready::task;
use dope_fiber::task::storage::set::insert;

use crate::common::schedule::tasks::fixtures::{set::failure::*, *};

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

#[test]
fn full_preserves_resident() {
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
            let tasks: Result<Set<'_, PendingCount, usize, Domain>, _> =
                Set::try_new(Count::new(1));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                let first = tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), 7, parent);
                let first = (first).expect("first task");

                assert!(matches!(
                    tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), 9, parent),
                    Err(Error::Full)
                ));
                assert_eq!(tasks.len().get(), 1);
                assert_eq!(tasks.read(&first, |value| *value), Some(7));

                assert!(tasks.remove(first).is_ok());
                let next = tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), 11, parent);
                let next = (next).expect("recycled vacancy");
                assert!(tasks.remove(next).is_ok());
                assert!(tasks.len() == Length::ZERO);
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn preserves_failed_vacancy() {
    let mut reports = Vec::new();

    Setup::for_profile::<Tight>().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch());
            let root = (root).expect("parent ready slot");
            let parent = RootWaker::from(root.target());
            let tasks: Result<Set<'_, PendingCount, (), Domain>, _> = Set::try_new(Count::new(1));
            let tasks = (tasks).expect("task set allocation");
            let mut tasks = pin!(tasks);
            Scope::with(tasks.as_mut(), |mut tasks| {
                {
                    let second = {
                        let first = core.ready().make_slot(
                            targets
                                .bind(SlotIndex::from(1_u16), Epoch::INITIAL)
                                .dispatch(),
                        );
                        assert!(first.is_ok(), "first blocking ready slot");
                        let second = core.ready().make_slot(
                            targets
                                .bind(SlotIndex::from(2_u16), Epoch::INITIAL)
                                .dispatch(),
                        );
                        assert!(matches!(
                            tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent),
                            Err(Error::Admission(task::Error::Capacity {
                                requested,
                                available: 0,
                            })) if requested.get() == 1
                        ));
                        assert!(tasks.len() == Length::ZERO);
                        second
                    };
                    assert!(second.is_ok(), "second blocking ready slot");
                    assert!(matches!(
                        tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent),
                        Err(Error::Topology(task::Error::Capacity {
                            requested,
                            available: 0,
                        })) if requested.get() == 1
                    ));
                    assert!(tasks.len() == Length::ZERO);
                }

                let task = tasks.try_insert(PendingCount(Rc::new(Cell::new(0))), (), parent);
                let task = (task).expect("failed preparations leave the vacancy reusable");
                assert!(tasks.remove(task).is_ok());
                assert!(tasks.len() == Length::ZERO);
            });
        },
        Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
