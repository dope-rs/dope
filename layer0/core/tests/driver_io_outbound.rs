use dope_core::{
    driver,
    driver::route::{Identity, KeyTag},
    io::fd::outbound::{Error, Reserved},
};
use o3::depot::arena::slab::Vacancy;

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
        assert!(matches!(held.bind(owner), Err(Vacancy::Full)));
        bound.push(slot);
    }
    assert_eq!(bound.len(), capacity.count().get());
}

#[test]
fn rejects_empty_slots() {
    Runtime::quic::<2, 8>().run(|scope| {
        let access = scope.context();
        assert!(matches!(
            Reserved::<KeyTag<ROUTE>>::try_new(&access, EMPTY),
            Err(Error::Empty)
        ));
    });
}

#[test]
fn reports_capacity() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .run(|scope| {
            let access = scope.context();
            assert!(matches!(
                Reserved::<KeyTag<ROUTE>>::try_new(&access, TWO),
                Err(Error::Capacity {
                    requested: 2,
                    available: 1,
                })
            ));
        });
}

#[test]
fn issues_owned_slots() {
    Runtime::quic::<2, 8>().run(|scope| {
        let access = scope.context();
        let held =
            (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("outbound reservation");
        let driver = access.driver_ref();
        assert!(
            held.bind(driver.tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO))
                .is_ok()
        );
        assert!(matches!(
            held.bind(driver.tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(1_u16))),
            Err(Vacancy::Full)
        ));
    });
}

#[test]
fn reclaims_unused() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .run(|scope| {
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
        });
}

#[test]
fn waits_for_last_fd() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 3>())
        .run(|scope| {
            scope.run_turn(|mut access, turn| {
                let [first, middle, last] = {
                    let held = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE))
                        .expect("first reservation");
                    [0_u16, 1, 2].map(|index| {
                        let owner = access
                            .driver_ref()
                            .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(index));
                        held.bind(owner).expect("bound descriptor")
                    })
                };
                for fd in [last, first, middle] {
                    assert!(
                        matches!(
                            Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, ONE),
                            Err(Error::Retiring {
                                requested: 1,
                                available: 0,
                            })
                        ),
                        "the range must remain retiring until its last descriptor returns"
                    );
                    drop(fd);
                }
                assert!(
                    matches!(
                        driver::Context::commit(&mut access, turn.reactor()),
                        Ok(Commit::Drained)
                    ),
                    "reclaim dropped descriptor slots"
                );

                let next = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE))
                    .expect("reactor turn must reclaim the last descriptor's slots");
                assert_capacity(&next, access.driver_ref(), THREE);
            });
        });
}

#[test]
fn releases_unsubmitted() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 300>())
        .run(|scope| {
            let access = scope.context();
            let held = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE_HUNDRED))
                .expect("outbound reservation");
            for local in 0_u16..300 {
                let owner = access
                    .driver_ref()
                    .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(local));
                let slot = held.bind(owner);
                assert!(slot.is_ok(), "bind outbound slot");
                assert!(matches!(held.bind(owner), Err(Vacancy::Full)));
            }
            let last = held.bind(
                access
                    .driver_ref()
                    .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(299_u16)),
            );
            assert!(last.is_ok(), "unsubmitted slot must release synchronously");
            assert!(matches!(
                held.bind(
                    access
                        .driver_ref()
                        .tuned_target::<KeyTag<ROUTE>>(SlotIndex::from(299_u16))
                ),
                Err(Vacancy::Full)
            ));
        });
}

#[test]
fn enforces_slot_authority() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 1>())
        .run(|scope| {
            let access = scope.context();
            let held =
                (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("outbound reservation");
            let owner = access
                .driver_ref()
                .tuned_target::<KeyTag<ROUTE>>(SlotIndex::ZERO);
            {
                let first = held.bind(owner);
                assert!(first.is_ok(), "first slot authority");
                assert!(matches!(held.bind(owner), Err(Vacancy::Full)));
            }
            let second = held.bind(owner);
            assert!(
                second.is_ok(),
                "slot must be reusable after authority release"
            );
            assert!(matches!(held.bind(owner), Err(Vacancy::Full)));
        });
}

#[test]
fn enforces_route_domain() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 2>())
        .run(|scope| {
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
            let reused = (Reserved::<KeyTag<ROUTE>>::try_new(&access, ONE)).expect("reused domain");
            assert_capacity(&reused, access.driver_ref(), ONE);
        });
}

#[test]
fn reuses_released_groups() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 8>())
        .run(|scope| {
            let access = scope.context();
            let (first, third) = {
                let first = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE)).expect("first");
                let second =
                    (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, TWO)).expect("second");
                let third =
                    (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, ONE)).expect("third");
                assert_capacity(&first, access.driver_ref(), THREE);
                assert_capacity(&second, access.driver_ref(), TWO);
                assert_capacity(&third, access.driver_ref(), ONE);
                (first, third)
            };

            let high = (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, ONE)).expect("high");
            let low = (Reserved::<KeyTag<{ ROUTE + 3 }>>::try_new(&access, ONE)).expect("low");
            assert_capacity(&high, access.driver_ref(), ONE);
            assert_capacity(&low, access.driver_ref(), ONE);
            assert_capacity(&first, access.driver_ref(), THREE);
            assert_capacity(&third, access.driver_ref(), ONE);
        });
}

#[test]
fn reuses_fragmented_groups() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 10>())
        .run(|scope| {
            let access = scope.context();
            let (upper, lowest, lower) = {
                let highest = (Reserved::<KeyTag<ROUTE>>::try_new(&access, TWO)).expect("highest");
                let upper =
                    (Reserved::<KeyTag<{ ROUTE + 1 }>>::try_new(&access, TWO)).expect("upper live");
                let lowest =
                    (Reserved::<KeyTag<{ ROUTE + 2 }>>::try_new(&access, TWO)).expect("lowest");
                let lower =
                    (Reserved::<KeyTag<{ ROUTE + 3 }>>::try_new(&access, TWO)).expect("lower live");
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
        });
}

#[test]
fn collects_free_slots() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<2, 8>())
        .run(|scope| {
            let access = scope.context();
            let (second, third) = {
                let first = (Reserved::<KeyTag<ROUTE>>::try_new(&access, THREE)).expect("first");
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
        });
}

#[test]
fn rolls_back_route() {
    Runtime::quic::<2, 8>().run(|scope| {
        let mut access = scope.context();
        assert!(
            Owner::<KeyTag<ROUTE>>::try_reserve(&mut access).is_ok(),
            "route transaction"
        );

        let vacant = (Owner::<KeyTag<ROUTE>>::try_reserve(&mut access)).expect("rolled-back route");
        let route = vacant.commit();
        assert!(matches!(
            Owner::<KeyTag<ROUTE>>::try_reserve(&mut access),
            Err(lease::Error::Occupied)
        ));

        route.release(&mut access);
        let vacant = (Owner::<KeyTag<ROUTE>>::try_reserve(&mut access)).expect("released route");
        let route = vacant.commit();
        route.release(&mut access);
    });
}
