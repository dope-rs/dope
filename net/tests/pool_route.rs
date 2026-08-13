use dope_core::driver::{
    lifecycle::routing::Route,
    ops,
    route::{self, table},
};
use dope_net::{
    link::{
        egress,
        pool::{Outbound, Pool, Prepared, input, raw::PreparedOutbound},
    },
    tcp::Tcp,
    wire::Identity,
};
use dope_test::scenario::rt::Runtime;

const ROUTE: u8 = 23;

type TestPool<'d> = Pool<'d, ROUTE, Tcp, Identity, (), input::Borrowed>;
type TestOutbound<'d> = Outbound<'d, ROUTE, Tcp, Identity, (), input::Borrowed>;
type TestPrepared<'d> = Prepared<'d, ROUTE, Tcp, Identity, (), input::Borrowed>;

#[test]
fn pool_modes_own_only_their_resources() {
    use std::mem::size_of;

    assert_eq!(
        size_of::<TestOutbound<'static>>(),
        size_of::<PreparedOutbound<'static, ROUTE, Tcp, Identity, (), input::Borrowed>>()
            + size_of::<Route<'static, ROUTE>>()
    );
    assert_eq!(
        size_of::<TestPool<'static>>(),
        size_of::<TestPrepared<'static>>() + size_of::<Route<'static, ROUTE>>()
    );
}

#[test]
fn outbound_preparation_derives_and_reclaims_the_pool_slots() {
    Runtime::throughput().with_driver_scope(|scope| {
        scope.with_turn(|_, mut driver, mut controller| {
            let mut turn = controller.begin(dope_core::driver::schedule::MAX_TURN_WORK_BUDGET);
            let capacity = table::Capacity::new(1).expect("single pool slot");
            let prepared = TestPrepared::new(capacity, 0, egress::Config::DEFAULT, (), &mut driver)
                .expect("prepare pool");
            let prepared =
                PreparedOutbound::reserve(prepared, &mut driver).expect("prepare outbound slots");
            drop(prepared);
            assert_eq!(
                ops::poll::Poll::commit(&mut driver, turn.reactor())
                    .expect("reclaim dropped outbound slots"),
                ops::poll::Commit::Drained
            );

            let reclaimed = ops::Files::reserve_outbound::<ROUTE>(&mut driver, 1)
                .expect("reuse reclaimed slots");
            ops::Files::retire_outbound(&mut driver, reclaimed);
            drop(turn);
        });
    });
}

#[test]
fn rolled_back_vacancy_rejects_its_late_completion_generation() {
    use dope_core::driver::route::KeyTag;

    type Tag = KeyTag<ROUTE>;

    let capacity = table::Capacity::new(1).expect("single pool slot");
    let mut slab = table::Slab::<(), Tag>::with_capacity(capacity);
    let stale = {
        let reservation = slab.vacant_entry().expect("first reservation");
        route::Token::from_key(reservation.key())
    };
    let current = {
        let reservation = slab.vacant_entry().expect("reused reservation");
        let current = route::Token::from_key(reservation.key());
        reservation.insert(());
        current
    };

    assert_eq!(stale.slot(), current.slot());
    assert_ne!(stale.epoch(), current.epoch());
    assert!(
        slab.entries()
            .at_parts(stale.parts::<Tag>().expect("stale tagged target"))
            .is_none()
    );
    assert!(
        slab.entries()
            .at_parts(current.parts::<Tag>().expect("current tagged target"))
            .is_some()
    );
}
