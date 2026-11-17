use std::{cell::Cell, num::NonZero, rc::Rc};

use dope::core::driver::{
    life::recovery::Policy,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule::ready::task::{self, owner},
};
use dope_fiber::{
    context::wake::RootWaker,
    task::storage::set::{Tasks, insert::Error},
};

pub mod common;
use common::{
    recover,
    runtime::{self, Setup},
    schedule::tasks::fixtures::{
        Domain, PendingCount,
        set::{Owner, failure::Tight},
    },
};

#[test]
fn failed_root_and_topology_release_same_vacancy() {
    let mut reports = Vec::new();
    Setup::for_profile::<Tight>().with_driver(
        |driver| {
            let core = driver.driver_ref();
            let targets = core.targets::<KeyTag<0>>();
            let root = core
                .ready()
                .make_slot(targets.bind(SlotIndex::ZERO, Epoch::INITIAL).dispatch())
                .expect("parent");
            let parent = RootWaker::from(root.target());
            Tasks::<'_, PendingCount, (), Domain>::with(
                NonZero::MIN,
                owner::Capacity::new(NonZero::MIN),
                |mut tasks| {
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
                                tasks.try_insert(
                                    PendingCount(Rc::new(Cell::new(0))),
                                    (),
                                    parent,
                                    Owner(0, 0),
                                ),
                                Err(Error::Admission(task::Error::Capacity {
                                    requested,
                                    available: 0,
                                })) if requested.get() == 1
                            ));
                            second
                        };
                        assert!(second.is_ok(), "second blocking ready slot");
                        assert!(matches!(
                            tasks.try_insert(
                                PendingCount(Rc::new(Cell::new(0))),
                                (),
                                parent,
                                Owner(0, 1),
                            ),
                            Err(Error::Topology(task::Error::Capacity {
                                requested,
                                available: 0,
                            })) if requested.get() == 1
                        ));
                    }
                    let task = tasks
                        .try_insert(
                            PendingCount(Rc::new(Cell::new(0))),
                            (),
                            parent,
                            Owner(0, 2),
                        )
                        .expect("same capacity and owner lane reusable");
                    tasks.discard(task);
                },
            )
            .expect("tasks");
        },
        Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    runtime::check_reports(&reports);
}
