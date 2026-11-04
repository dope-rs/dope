use dope_core::driver::{
    life::recovery,
    route::{Identity, KeyTag},
};

use crate::common::contract::Scope;
pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use dope_core::driver::{Context, route::lease, schedule::Progress};
use dope_test::alloc::Tracker;

use crate::common::rt::{Runtime, finish, recover};

#[test]
fn repeated_quiescence_preserves_config_without_allocating() {
    let config = Runtime::quic::<2, 8>().config();
    let receive = config.receive();
    let mut events = Vec::new();

    Runtime { config }.with_scope(
        |scope| {
            scope.quiesce();
            let ((), alloc) = Tracker::usage(|| {
                scope.quiesce();
                assert!(scope.context().receive() == receive);
                scope.quiesce();
            });
            assert_eq!(alloc, (0, 0));
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );
}

#[test]
fn retains_live_route() {
    let mut events = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            {
                let owner = {
                    let mut driver = scope.context();
                    let vacant = (lease::Owner::<KeyTag<17>>::try_reserve(&mut driver))
                        .expect("the route starts vacant");
                    vacant.into_claim().store([1, 2, 3])
                };
                let ((), alloc) = Tracker::usage(|| scope.finish(false, |_, _| ()));
                assert_eq!(alloc, (0, 0));
                assert_eq!(*owner, [1, 2, 3]);
                assert!(matches!(
                    lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context()),
                    Err(lease::Error::Occupied)
                ));
            }
            scope.quiesce();
            assert!(lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context()).is_ok());
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );
}

#[test]
fn retains_deferred_route() {
    let mut events = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            for owned in [false, true] {
                let retired = {
                    let claim = lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context())
                        .expect("the prior retirement was drained")
                        .into_claim();
                    if owned {
                        let owner = claim.into_owner();
                        owner.driver()
                    } else {
                        claim.driver()
                    }
                };
                assert!(matches!(retired.maintenance(), Progress::Runnable));
                let error = lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context())
                    .err()
                    .expect("queued retirement retains the route reservation");
                assert!(matches!(error, lease::Error::Occupied));
                scope.quiesce();
            }
            let mut driver = scope.context();
            let owner = lease::Owner::<KeyTag<17>>::try_reserve(&mut driver)
                .expect("both deferred owners were released")
                .commit();
            owner.release(&mut driver);
            let owner = lease::Owner::<KeyTag<17>>::try_reserve(&mut driver)
                .expect("immediate release consumes its defer authority")
                .commit();
            owner.release(&mut driver);
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );
}

#[test]
fn retires_route_words() {
    let mut events = Vec::new();

    fn defer<ID: Identity>(driver: &mut Context<'_, '_>) {
        let retired = {
            let claim = lease::Owner::<ID>::try_reserve(driver)
                .expect("the previous retirement released this route")
                .into_claim();
            claim.driver()
        };
        assert!(matches!(retired.maintenance(), Progress::Runnable));
        assert!(matches!(
            lease::Owner::<ID>::try_reserve(driver),
            Err(lease::Error::Occupied)
        ));
    }

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            for _ in 0..2 {
                {
                    let mut driver = scope.context();
                    defer::<KeyTag<254>>(&mut driver);
                    defer::<KeyTag<224>>(&mut driver);
                    defer::<KeyTag<223>>(&mut driver);
                    defer::<KeyTag<192>>(&mut driver);
                    defer::<KeyTag<191>>(&mut driver);
                    defer::<KeyTag<160>>(&mut driver);
                    defer::<KeyTag<159>>(&mut driver);
                    defer::<KeyTag<128>>(&mut driver);
                    defer::<KeyTag<127>>(&mut driver);
                    defer::<KeyTag<96>>(&mut driver);
                    defer::<KeyTag<95>>(&mut driver);
                    defer::<KeyTag<64>>(&mut driver);
                    defer::<KeyTag<63>>(&mut driver);
                    defer::<KeyTag<32>>(&mut driver);
                    defer::<KeyTag<31>>(&mut driver);
                    defer::<KeyTag<0>>(&mut driver);
                }
                scope.quiesce();
            }
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );
}

#[test]
fn retains_dropping_value() {
    let mut events = Vec::new();

    Runtime::quic::<2, 8>().with_scope(
        |scope| {
            let retired = {
                let claim = lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context())
                    .expect("the protected value can reserve its route")
                    .into_claim();
                let owner = claim.store(Scope(|| {
                    scope.quiesce();
                    let error = lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context())
                        .err()
                        .expect("the value's destructor still owns the route");
                    assert!(matches!(error, lease::Error::Occupied));
                }));
                owner.driver()
            };
            assert!(matches!(retired.maintenance(), Progress::Runnable));
            scope.quiesce();
            assert!(lease::Owner::<KeyTag<17>>::try_reserve(&mut scope.context()).is_ok());
        },
        recovery::Policy::new(|event| events.push(event), recover),
        finish,
    );
    assert!(
        events.is_empty(),
        "unexpected reclaimed operations: {events:?}"
    );
}
