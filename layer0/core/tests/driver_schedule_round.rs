use std::convert::Infallible;

use dope_core::driver::{
    life::{creation, recovery},
    route::KeyTag,
};

use crate::common::{
    driver::{Scoped as _, support::Configure as _},
    wake::Ready as _,
};

pub mod common;

use std::num::NonZero;

use dope_core::driver::{
    life::domain,
    route::{Epoch, indices::SlotIndex},
    schedule, settings,
};

use crate::common::{driver::support::ROUTE, rt::Runtime};

#[test]
fn coalesced_wake() {
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(
        Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE),
    );
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.run_turn(|context, turn| {
                        let dref = context.driver_ref();
                        let targets = [0_u16, 1].map(|slot| {
                            dref.targets::<KeyTag<ROUTE>>()
                                .bind(SlotIndex::from(slot), Epoch::INITIAL)
                                .dispatch()
                        });
                        let [first, second] = targets
                            .map(|target| dref.ready().make_slot(target).expect("two ready slots"));
                        first.activate();
                        second.activate();

                        let mut drained = Vec::new();
                        turn.ready_round(|token| {
                            drained.push(token);
                            if targets[0].matches(token) {
                                second.activate();
                            }
                        });

                        assert_eq!(drained.len(), 2);
                        assert!(targets[0].matches(drained[0]));
                        assert!(targets[1].matches(drained[1]));
                        assert!(!dref.ready().has_ready());
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
fn next_round() {
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(
        Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE),
    );
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.run_turn(|context, turn| {
                        let dref = context.driver_ref();
                        let target = dref
                            .targets::<KeyTag<ROUTE>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch();
                        let slot = (dref.ready().make_slot(target)).expect("ready slot");
                        slot.activate();

                        let mut rounds = turn.ready();
                        let mut first = Vec::new();
                        assert_eq!(
                            rounds.drain(|ready| {
                                first.push(ready);
                                slot.activate();
                            }),
                            Some(NonZero::<usize>::MIN)
                        );
                        let mut second = Vec::new();
                        assert_eq!(
                            rounds.drain(|ready| second.push(ready)),
                            Some(NonZero::<usize>::MIN)
                        );

                        assert_eq!(first.len(), 1);
                        assert!(target.matches(first[0]));
                        assert_eq!(second.len(), 1);
                        assert!(target.matches(second[0]));
                        assert!(!dref.ready().has_ready());
                        assert!(!rounds.is_exhausted());
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
fn nested_budget() {
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(
        (Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE))
        .with_ready(schedule::LIMIT + 1)
        .expect("ready capacity"),
    );
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|context, mut turn| {
                        let dref = context.driver_ref();
                        let targets: Vec<_> = (0..schedule::LIMIT + 1)
                            .map(|slot| {
                                dref.targets::<KeyTag<ROUTE>>()
                                    .bind(SlotIndex::from(slot as u16), Epoch::INITIAL)
                                    .dispatch()
                            })
                            .collect();
                        let slots = (targets
                            .iter()
                            .copied()
                            .map(|target| dref.ready().make_slot(target))
                            .collect::<Result<Vec<_>, _>>()
                            .map(Vec::into_boxed_slice))
                        .expect("ready slots");
                        for slot in &slots[..schedule::LIMIT] {
                            slot.activate();
                        }

                        {
                            let active = turn.begin();
                            let mut rounds = active.ready();
                            let mut outer = Vec::new();
                            let mut nested = Vec::new();
                            let drained = rounds.drain(|ready| {
                                outer.push(ready);
                                if outer.len() == 1 {
                                    slots[schedule::LIMIT].activate();
                                    active.ready_round(|ready| nested.push(ready));
                                }
                            });
                            assert_eq!(drained, NonZero::<usize>::new(schedule::LIMIT));
                            assert!(nested.is_empty());
                            assert!(rounds.is_exhausted());
                            assert!(dref.ready().has_ready());
                            assert!(rounds.drain(|_| panic!("exhausted ready")).is_none());
                        }

                        let active = turn.begin();
                        let mut deferred = Vec::new();
                        assert_eq!(active.ready_round(|ready| deferred.push(ready)), 1);
                        assert_eq!(deferred.len(), 1);
                        assert!(targets[schedule::LIMIT].matches(deferred[0]));
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
fn drop_unlinks() {
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(
        Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE),
    );
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.run_turn(|context, turn| {
                        let dref = context.driver_ref();
                        let targets = [0_u16, 1, 2].map(|slot| {
                            dref.targets::<KeyTag<ROUTE>>()
                                .bind(SlotIndex::from(slot), Epoch::INITIAL)
                                .dispatch()
                        });
                        let [first, second, third] = targets.map(|target| {
                            dref.ready().make_slot(target).expect("three ready slots")
                        });
                        first.activate();
                        second.activate();
                        third.activate();
                        let mut second = Some(second);
                        let mut drained = Vec::new();
                        turn.ready_round(|token| {
                            drained.push(token);
                            if targets[0].matches(token) {
                                let slot = second.take();
                                assert!(slot.is_some(), "activated second slot remains owned");
                            }
                        });
                        assert_eq!(drained.len(), 2);
                        assert!(targets[0].matches(drained[0]));
                        assert!(targets[2].matches(drained[1]));
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
