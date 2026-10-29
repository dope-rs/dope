use std::convert::Infallible;

use dope_core::driver::life::{creation, recovery};
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::num;

use dope_core::driver::{
    life::domain,
    route::{Epoch, KeyTag, indices::SlotIndex},
    settings,
    settings::demands::Schedule,
};
use dope_test::alloc::Tracker;
use settings::demands::slots;

use crate::common::rt::Runtime;

#[test]
fn drains_empty() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE)
        .with_files(settings::FileSlots::fixed::<0, 1>());
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        slots::Slots::ZERO,
        config.schedule().timers(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation
        .try_driver(config)
        .map_err(|error| error.kind())
        .expect("driver construction");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|context, mut control| {
                    let site = context.driver_ref();
                    let key = site
                        .targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch();
                    assert!(
                        matches!(site.ready().make_slot(key), Err(error) if error.available() == 0)
                    );
                    let ((), usage) = Tracker::usage(|| {
                        for _ in 0..4 {
                            let turn = control.begin();
                            let mut rounds = turn.ready();
                            assert!(!site.ready().has_ready());
                            let mut emitted = 0;
                            let drained = rounds.drain(|_| emitted += 1);
                            assert_eq!((drained, emitted), (None, 0));
                        }
                    });
                    assert_eq!(usage, (0, 0));
                });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn preserves_sparse_rounds() {
    for count in [1, 30, 31, 32, 2046, 2047, 2048, 65_534] {
        let capacity = (slots::Slots::try_from(count)).expect("ready capacity");
        let config = Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE)
            .with_files(settings::FileSlots::fixed::<0, 1>());
        let config = config.set_schedule(
            settings::Schedule::fixed::<0, 0>()
                .resolve(Schedule::new(capacity, config.schedule().timers())),
        );
        let mut creation = creation::Attempt::new();
        let driver = creation
            .try_driver(config)
            .map_err(|error| error.kind())
            .expect("driver construction for sparse rounds");
        let mut reports = Vec::new();

        assert!(
            domain::Domain::new(driver)
                .start()
                .expect("fresh generation")
                .enter_with(
                    (),
                    |mut scope, _, _| {
                        scope.with_driver(|context, mut control| {
                            let site = context.driver_ref();
                            let targets: Vec<_> = (0..count)
                                .map(|index| {
                                    site.targets::<KeyTag<0>>()
                                        .bind(SlotIndex::from(index as u16), Epoch::INITIAL)
                                        .dispatch()
                                })
                                .collect();
                            let held = (targets
                                .iter()
                                .copied()
                                .map(|target| site.ready().make_slot(target))
                                .collect::<Result<Vec<_>, _>>()
                                .map(Vec::into_boxed_slice))
                            .expect("complete ready capacity");
                            let mut indices: Vec<_> = [0, 30, 31, 2046, 2047, count - 1]
                                .into_iter()
                                .filter(|index| *index < count)
                                .collect();
                            indices.sort_unstable();
                            indices.dedup();
                            let ((), usage) = Tracker::usage(|| {
                                for _ in 0..4 {
                                    for &index in &indices {
                                        held[index].activate();
                                        held[index].activate();
                                    }
                                    let turn = control.begin();
                                    let mut next = 0;
                                    turn.ready().drain(|token| {
                                        assert!(targets[indices[next]].matches(token));
                                        if next == 0 {
                                            held[0].activate();
                                        }
                                        next += 1;
                                    });
                                    assert_eq!(next, indices.len());
                                    assert_eq!(
                                        turn.ready()
                                            .drain(|token| assert!(targets[0].matches(token))),
                                        Some(num::NonZero::<usize>::MIN)
                                    );
                                    assert!(!site.ready().has_ready());
                                }
                            });
                            assert_eq!(usage, (0, 0));
                        });
                    },
                    recovery::Policy::new(|event| reports.push(event), recover)
                )
                .is_ok()
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
