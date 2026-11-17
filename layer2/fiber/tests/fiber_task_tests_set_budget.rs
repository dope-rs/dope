use std::{cell::Cell, num::NonZero, rc::Rc};

use dope::core::driver::{
    life::recovery::Policy,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::{self, ready::task::owner},
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::set::Tasks,
};

pub mod common;
use common::{
    recover,
    runtime::{self, Setup},
    schedule::tasks::fixtures::{Domain, PendingCount, Rounds as _, set::Owner},
};

#[test]
fn resumes_after_budget() {
    const TASKS: usize = schedule::LIMIT + 2;
    let mut reports = Vec::new();
    Setup::throughput().with_scope(
        |scope| {
            let (_, mut driver, mut control) = scope.split();
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
            Tasks::<'_, PendingCount, (), Domain>::with(
                NonZero::new(TASKS as u32).expect("capacity"),
                owner::Capacity::new(NonZero::MIN),
                |mut tasks| {
                    for _ in 0..TASKS {
                        let id = tasks
                            .try_insert(PendingCount(Rc::clone(&polls)), (), parent, Owner(0, 0))
                            .expect("insert");
                        drop(id);
                    }
                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 1);
                        tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, _| {});
                        assert_eq!(polls.get(), schedule::LIMIT);
                        assert!(tasks.has_work_for(Owner(0, 0)));
                    }
                    {
                        let turn = control.begin();
                        assert_eq!(turn.ready_round(|_| {}), 1);
                        tasks.poll_with(Owner(0, 0), turn.turn().app(), &mut driver, |_, _, _| {});
                        assert_eq!(polls.get(), TASKS);
                        assert!(!tasks.has_work_for(Owner(0, 0)));
                    }
                    assert_eq!(control.begin().ready_round(|_| {}), 0);
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}
