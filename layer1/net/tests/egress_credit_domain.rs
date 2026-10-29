pub mod common;

use std::convert::Infallible;

use common::{
    credit::{Error, open, reclaimed, release_one, with_pool},
    rt::throughput,
};
use dope_core::driver::{
    self,
    life::recovery,
    route::{KeyTag, table::conn},
};
use dope_net::{
    link::{
        egress::{
            self,
            data::compose::Either,
            reserve::demand::{self, Mode},
        },
        pool::{
            change,
            pending::{Disposition, Pending},
            recv::input,
        },
    },
    tcp::Tcp,
    wire::{
        Identity,
        domain::{Local, Resident, Retained},
    },
};
use dope_test::scope;
use o3::buffer::{pool, resident::bound};

#[test]
fn send_wait_reuse() {
    with_pool::<2>(
        egress::Config::DEFAULT,
        |pool, [retired, live], driver, turn| {
            let waiting = Pending::of(pool).at(retired).expect("old generation");
            waiting.wait_send(true);
            waiting.wake();
            Pending::of(pool)
                .at(live)
                .expect("live generation")
                .wait_send(true);

            assert!(matches!(
                release_one(pool, retired, driver, turn),
                Ok(false)
            ));
            assert!(Pending::of(pool).at(retired).is_none());
            assert!(
                Pending::of(pool)
                    .at(live)
                    .expect("live generation")
                    .has_capacity()
            );

            let next = open(pool, conn::Capacity::fixed::<2>(), driver).expect("reused slot");
            assert!(next != retired);
            assert!(
                !Pending::of(pool)
                    .at(next)
                    .expect("new generation")
                    .has_capacity()
            );
            let mut wakes = 0;
            Pending::of(pool)
                .try_round()
                .expect("pending round")
                .run(turn.maint(), |key| {
                    let handle = Pending::of(pool).at(key).expect("current generation");
                    if key == live {
                        assert!(handle.has_capacity());
                        wakes += 1;
                        handle.wait_send(false);
                    } else {
                        assert!(key == next);
                        assert!(!handle.has_capacity());
                    }
                    Disposition::Idle
                });
            assert_eq!(wakes, 1);
            assert!(Pending::of(pool).is_empty());
        },
        recovery::Policy::new(
            reclaimed,
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("native lifecycle");
            result.ok().expect("driver output").expect("credit fixture");
        },
    );
}

#[test]
fn guaranteed_queue_proof() {
    for (credit, queued) in [
        (egress::Credit::fixed::<0, 0>(), false),
        (egress::Credit::fixed::<2, 1>(), true),
    ] {
        throughput().with_scope(
            |scope| {
                scope.with_driver(|mut driver, _| {
                    let cap = conn::Capacity::fixed::<1>();
                    let setup = match demand::Guaranteed::<bound::AtMost<0>, 1>::provision(
                        egress::Config::shared(credit),
                        cap,
                    ) {
                        demand::Provision::Queued { setup, .. } => {
                            assert!(queued);
                            setup
                        }
                        demand::Provision::Direct(setup) => {
                            assert!(!queued);
                            setup
                        }
                    };
                    let result = change::try_prepare::<
                        KeyTag<23>,
                        Tcp,
                        Identity,
                        (),
                        input::Borrowed,
                        Either<&[u8], pool::Cursor>,
                        32,
                        _,
                        _,
                        _,
                        _,
                    >(
                        Retained::ZERO,
                        Resident::ZERO,
                        setup,
                        Local::new(Identity),
                        &mut driver,
                        driver::Context::reborrow,
                        |(prepared, need), driver| {
                            let flights = driver
                                .storage
                                .reborrow()
                                .try_flights(need)
                                .expect("flight storage")
                                .slots();
                            let route = driver
                                .storage
                                .reborrow()
                                .try_reserve::<KeyTag<23>>()
                                .expect("route owner");
                            // SAFETY: no send, receive or tuning operation borrows this pool.
                            let mut pool = unsafe { prepared.commit(flights).bind(route) };
                            let key = open(&mut pool, cap, driver).expect("connection");
                            let queue = pool.egress_mut(key).expect("queue").queue;
                            let plan = queue.prove();
                            assert_eq!(plan.is_some(), queued);
                            if let Some(plan) = plan {
                                for _ in 0..2 {
                                    plan.try_reserve(driver.region_token())
                                        .expect("admitted queue capacity")
                                        .cancel();
                                }
                            }
                        },
                    );
                    assert!(result.is_ok(), "pool storage preparation failed");
                    Ok::<_, Error>(())
                })
            },
            recovery::Policy::new(
                reclaimed,
                |failure: recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("driver recovery: {:?}", failure.cause())
                },
            ),
            |result| {
                result
                    .as_ref()
                    .map_err(scope::Error::cause)
                    .expect("native lifecycle");
                result.ok().expect("driver output").expect("credit fixture");
            },
        );
    }
}
