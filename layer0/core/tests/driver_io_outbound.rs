use std::convert::Infallible;

use dope_core::{
    driver,
    driver::{
        life::recovery,
        route::{Identity, KeyTag},
    },
    io::fd::outbound::{Error, Reserved},
};
use dope_test::scope;

use crate::common::driver::{Scoped as _, support::Targets as _};

pub mod common;

use dope_core::driver::{
    result::poll::Commit,
    route::{indices::SlotIndex, lease, lease::Owner, table::Capacity},
    settings,
};

use crate::common::{driver::support::ROUTE, rt::Runtime};

const EMPTY: Capacity = settings::FileSlots::fixed::<0, 1>().accept();
const ONE: Capacity = settings::FileSlots::fixed::<0, 1>().outbound();
const TWO: Capacity = settings::FileSlots::fixed::<0, 2>().outbound();
const THREE: Capacity = settings::FileSlots::fixed::<0, 3>().outbound();
const EIGHT: Capacity = settings::FileSlots::fixed::<0, 8>().outbound();
const THREE_HUNDRED: Capacity = settings::FileSlots::fixed::<0, 300>().outbound();

fn assert_capacity<'d, ID: Identity>(
    held: &Reserved<'d, ID>,
    driver: driver::Reference<'d>,
    capacity: Capacity,
) {
    let mut bound = Vec::new();
    for index in capacity.into_iter() {
        let owner = driver.tuned_target::<ID>(index);
        let slot = (held.bind(owner)).expect("reserved outbound slot");
        assert!(held.bind(owner).is_none());
        bound.push(slot);
    }
    assert_eq!(bound.len(), capacity.count().get());
}

#[test]
fn rejects_empty_slots() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let access = scope.context();
            assert!(matches!(
                Reserved::<KeyTag<ROUTE>>::try_new(&access, EMPTY),
                Err(Error::Empty)
            ));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reports_capacity() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .with_scope(
            |scope| {
                let access = scope.context();
                assert!(matches!(
                    Reserved::<KeyTag<ROUTE>>::try_new(&access, TWO),
                    Err(Error::Capacity {
                        requested: 2,
                        available: 1,
                    })
                ));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn issues_owned_slots() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let access = scope.context();
            let held =
                (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("outbound reservation");
            let driver = access.driver_ref();
            assert!(
                held.bind(driver.tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO))
                    .is_some()
            );
            assert!(
                held.bind(driver.tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(1_u16)))
                    .is_none()
            );
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reclaims_unused() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .with_scope(
            |scope| {
                scope.run_turn(|mut access, turn| {
                    {
                        let first = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE))
                            .expect("first reservation");
                        assert_capacity(&first, access.driver_ref(), ONE);
                    }
                    assert!(
                        matches!(
                            driver::Context::commit(&mut access, turn.reactor()),
                            Ok(Commit::Drained)
                        ),
                        "reclaim dropped reservation"
                    );

                    let second = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE))
                        .expect("reactor turn must reclaim the dropped reservation");
                    assert_capacity(&second, access.driver_ref(), ONE);
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn waits_for_last_fd() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .with_scope(
            |scope| {
                scope.run_turn(|mut access, turn| {
                    {
                        let owner = access
                            .driver_ref()
                            .tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO);
                        let fd = {
                            let held = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE))
                                .expect("first reservation");
                            held.bind(owner)
                        };
                        assert!(fd.is_some(), "bound descriptor");

                        assert!(
                            matches!(
                                Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, ONE),
                                Err(Error::Retiring {
                                    requested: 1,
                                    available: 0,
                                })
                            ),
                            "the slots must remain retiring while their descriptor is live"
                        );
                    }
                    assert!(
                        matches!(
                            driver::Context::commit(&mut access, turn.reactor()),
                            Ok(Commit::Drained)
                        ),
                        "reclaim dropped descriptor slots"
                    );

                    let next = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE))
                        .expect("reactor turn must reclaim the last descriptor's slots");
                    assert_capacity(&next, access.driver_ref(), ONE);
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn releases_unsubmitted() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 300>())
        .with_scope(
            |scope| {
                let access = scope.context();
                let held = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE_HUNDRED))
                    .expect("outbound reservation");
                for local in 0_u16..300 {
                    let owner = access
                        .driver_ref()
                        .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(local));
                    let slot = held.bind(owner);
                    assert!(slot.is_some(), "bind outbound slot");
                    assert!(held.bind(owner).is_none());
                }
                let last = held.bind(
                    access
                        .driver_ref()
                        .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(299_u16)),
                );
                assert!(
                    last.is_some(),
                    "unsubmitted slot must release synchronously"
                );
                assert!(
                    held.bind(
                        access
                            .driver_ref()
                            .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(299_u16))
                    )
                    .is_none()
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn enforces_slot_authority() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .with_scope(
            |scope| {
                let access = scope.context();
                let held = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE))
                    .expect("outbound reservation");
                let owner = access
                    .driver_ref()
                    .tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO);
                {
                    let first = held.bind(owner);
                    assert!(first.is_some(), "first slot authority");
                    assert!(held.bind(owner).is_none());
                }
                let second = held.bind(owner);
                assert!(
                    second.is_some(),
                    "slot must be reusable after authority release"
                );
                assert!(held.bind(owner).is_none());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn enforces_route_domain() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 2>())
        .with_scope(
            |scope| {
                let access = scope.context();
                {
                    let first =
                        (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("first domain");
                    assert!(
                        matches!(
                            Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE),
                            Err(Error::Occupied)
                        ),
                        "one route cannot own two outbound domains"
                    );
                    assert_capacity(&first, access.driver_ref(), ONE);
                }
                let reused =
                    (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("reused domain");
                assert_capacity(&reused, access.driver_ref(), ONE);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reuses_released_groups() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 8>())
        .with_scope(
            |scope| {
                let access = scope.context();
                let (first, third) = {
                    let first =
                        (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE)).expect("first");
                    let second =
                        (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, TWO)).expect("second");
                    let third =
                        (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, ONE)).expect("third");
                    assert_capacity(&first, access.driver_ref(), THREE);
                    assert_capacity(&second, access.driver_ref(), TWO);
                    assert_capacity(&third, access.driver_ref(), ONE);
                    (first, third)
                };

                let high =
                    (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, ONE)).expect("high");
                let low = (Reserved::<KeyTag<{ ROUTE + 3 }>>::try_new(&access, ONE)).expect("low");
                assert_capacity(&high, access.driver_ref(), ONE);
                assert_capacity(&low, access.driver_ref(), ONE);
                assert_capacity(&first, access.driver_ref(), THREE);
                assert_capacity(&third, access.driver_ref(), ONE);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reuses_fragmented_groups() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 10>())
        .with_scope(
            |scope| {
                let access = scope.context();
                let (upper, lowest, lower) = {
                    let highest =
                        (Reserved::<KeyTag<ROUTE>>::try_new(&access, TWO)).expect("highest");
                    let upper = (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, TWO))
                        .expect("upper live");
                    let lowest =
                        (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, TWO)).expect("lowest");
                    let lower = (Reserved::<KeyTag<{ ROUTE + 3 }>>::try_new(&access, TWO))
                        .expect("lower live");
                    assert_capacity(&highest, access.driver_ref(), TWO);
                    assert_capacity(&upper, access.driver_ref(), TWO);
                    assert_capacity(&lowest, access.driver_ref(), TWO);
                    assert_capacity(&lower, access.driver_ref(), TWO);
                    (upper, lowest, lower)
                };
                assert_capacity(&lowest, access.driver_ref(), TWO);
                drop(lowest);

                let first =
                    (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("first singleton");
                let second = (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, ONE))
                    .expect("second singleton");
                let hole =
                    (Reserved::<KeyTag<{ ROUTE + 4 }>>::try_new(&access, TWO)).expect("high hole");
                assert_capacity(&first, access.driver_ref(), ONE);
                assert_capacity(&second, access.driver_ref(), ONE);
                assert_capacity(&hole, access.driver_ref(), TWO);
                assert_capacity(&upper, access.driver_ref(), TWO);
                assert_capacity(&lower, access.driver_ref(), TWO);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn collects_free_slots() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 8>())
        .with_scope(
            |scope| {
                let access = scope.context();
                let (second, third) = {
                    let first =
                        (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE)).expect("first");
                    let second =
                        (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, TWO)).expect("second");
                    let third =
                        (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, ONE)).expect("third");
                    assert_capacity(&first, access.driver_ref(), THREE);
                    (second, third)
                };
                assert_capacity(&second, access.driver_ref(), TWO);
                drop(second);
                assert_capacity(&third, access.driver_ref(), ONE);
                drop(third);

                let held =
                    (Reserved::<KeyTag<ROUTE>>::try_new(&access, EIGHT)).expect("fully reclaimed");
                assert_capacity(&held, access.driver_ref(), EIGHT);
                assert_capacity(&held, access.driver_ref(), EIGHT);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            finish,
        );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn rolls_back_route() {
    let mut reports = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let mut access = scope.context();
            assert!(
                Owner::<KeyTag<ROUTE>>::try_reserve(&mut access).is_ok(),
                "route transaction"
            );

            let vacant =
                (Owner::<KeyTag<ROUTE>>::try_reserve(&mut access)).expect("rolled-back route");
            let route = vacant.commit();
            assert!(matches!(
                Owner::<KeyTag<ROUTE>>::try_reserve(&mut access),
                Err(lease::Error::Occupied)
            ));

            route.release(&mut access);
            let vacant =
                (Owner::<KeyTag<ROUTE>>::try_reserve(&mut access)).expect("released route");
            let route = vacant.commit();
            route.release(&mut access);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), scope::Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(scope::Error::cause)
        .expect("driver lifecycle");
}
