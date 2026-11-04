pub mod common;

use std::task::Poll;

use common::{
    chain::Fill,
    credit::{release_one, with_pool},
};
use dope_core::driver::schedule::{self, ready::Readiness};
use dope_net::link::egress::{
    self,
    data::compose::Either,
    metadata::Lane,
    reserve::{Bounds, limit::Limit},
};
use o3::buffer::pool;

#[test]
fn overflow_wait_refund() {
    with_pool::<1>(
        egress::Config::shared(egress::Credit::fixed::<2, { u32::MAX }>()),
        |pool, [key], driver, turn| {
            let bytes = pool::Pool::try_new(1, 1).expect("payload pool");
            let ready = driver
                .driver_ref()
                .ready()
                .make_slot(key.target().dispatch())
                .expect("ready slot");
            let bounds = Bounds::one(Limit::at_most::<1, { u32::MAX }>());
            {
                let queue = pool.egress_mut(key).expect("connection").queue;
                assert!(matches!(
                    queue.try_enqueue(
                        driver.region_token(),
                        Either::Right(bytes.try_cursor(b"x").expect("payload")),
                    ),
                    egress::Enqueue::Committed
                ));
                assert!(
                    queue
                        .plan(bounds)
                        .expect("bounds")
                        .poll_reserve(driver.region_token(), ready.target())
                        .is_pending()
                );
            }
            assert!(matches!(release_one(pool, key, driver, turn), Ok(true)));
            let queue = pool.egress_mut(key).expect("connection").queue;
            let grant = match queue
                .plan(bounds)
                .expect("bounds")
                .poll_reserve(driver.region_token(), ready.target())
            {
                Poll::Ready(result) => Some(result),
                Poll::Pending => None,
            };
            grant
                .expect("refunded credit is ready")
                .expect("credit reservation")
                .cancel();
        },
    );
}

#[test]
fn pair_entry_wake() {
    with_pool::<2>(
        egress::Config::shared(egress::Credit::fixed::<2, 0>()),
        |pool, [a, b], driver, turn| {
            let ready = driver
                .driver_ref()
                .ready()
                .make_slot(b.target().dispatch())
                .expect("ready slot");
            assert!(matches!(
                pool.egress_mut(a)
                    .expect("connection")
                    .queue
                    .try_enqueue(driver.region_token(), Either::Left(b"x"),),
                egress::Enqueue::Committed
            ));
            let bounds = Bounds::pair(Limit::at_most::<1, 0>(), Limit::at_most::<1, 0>())
                .expect("pair bounds");
            assert!(
                pool.egress_mut(b)
                    .expect("connection")
                    .queue
                    .plan(bounds)
                    .expect("bounds")
                    .poll_reserve(driver.region_token(), ready.target())
                    .is_pending()
            );
            assert!(matches!(release_one(pool, a, driver, turn), Ok(true)));
            pool.credit_step(driver.region_token(), turn.maint());
            assert!(matches!(ready.take(), Readiness::Ready));
            let queue = pool.egress_mut(b).expect("connection").queue;
            let grant = match queue
                .plan(bounds)
                .expect("bounds")
                .poll_reserve(driver.region_token(), ready.target())
            {
                Poll::Ready(result) => Some(result),
                Poll::Pending => None,
            };
            grant
                .expect("pair credit is ready")
                .expect("pair reservation")
                .cancel();
        },
    );
}

#[test]
fn pair_returns_owners() {
    use egress::{
        load::Value,
        reserve::{
            error::{Cause, Mismatch},
            limit,
        },
    };

    with_pool::<1>(
        egress::Config::shared(egress::Credit::fixed::<2, 4>()),
        |pool, [key], driver, _| {
            let bytes = pool::Pool::try_new(2, 2).expect("payload pool");
            for (limit, resident) in [
                (Some(Limit::at_most::<1, 2>()), false),
                (Some(Limit::at_most::<2, 1>()), true),
                (None, false),
            ] {
                let input: [&[u8]; 2] = if limit.is_some() {
                    [b"x", b"yz"]
                } else {
                    [b"xx", b"y"]
                };
                let owners = input.map(|input| bytes.try_cursor(input).expect("payload"));
                let pointers = owners.each_ref().map(|owner| owner.as_ref().as_ptr());
                let [first, second] =
                    owners.map(|owner| Value::Unchecked(egress::Load::new(Either::Right(owner))));
                let bounds = match limit {
                    Some(limit) => {
                        Bounds::pair(Limit::at_most::<1, 2>(), limit).expect("pair bounds")
                    }
                    None => Bounds::one(Limit::at_most::<1, 2>()),
                };
                let queue = pool.egress_mut(key).expect("connection").queue;
                let plan = queue.plan(bounds).expect("queue bounds");
                let rejected = plan
                    .try_reserve(driver.region_token())
                    .expect("reservation")
                    .try_publish(Some(first), Some(second))
                    .err()
                    .expect("publication rejected");
                let (cause, _, first, second) = rejected.into_parts();
                assert!(match cause {
                    Cause::Changed(Mismatch::Second(limit::Mismatch::Maximum {
                        maximum,
                        actual,
                    })) => {
                        limit.is_some()
                            && !resident
                            && maximum.widen().get() == 1
                            && actual.widen().get() == 2
                    }
                    Cause::Changed(Mismatch::Second(limit::Mismatch::Credit(
                        egress::Shortage::Resident(cause),
                    ))) => resident && cause.available().raw() == 1 && cause.requested().raw() == 2,
                    Cause::Changed(Mismatch::UnexpectedSecond { wire, resident }) => {
                        limit.is_none() && wire.widen().get() == 1 && resident.raw() == 2
                    }
                    _ => false,
                });
                let owners = [first, second].map(|value| {
                    let load = match value {
                        Some(Value::Checked(load)) => Some(load),
                        _ => None,
                    };
                    let owner = match load.expect("checked owner").into_inner() {
                        Either::Right(owner) => Some(owner),
                        Either::Left(_) => None,
                    };
                    owner.expect("cursor owner")
                });
                assert_eq!(
                    owners.each_ref().map(|owner| owner.as_ref().as_ptr()),
                    pointers
                );
                assert_eq!(owners.each_ref().map(|owner| owner.as_ref()), input);
                assert_eq!(bytes.available(), 0);
                assert!(queue.is_empty());
                let full = Bounds::pair(Limit::at_most::<1, 2>(), Limit::at_most::<2, 2>())
                    .expect("full bounds");
                queue
                    .plan(full)
                    .expect("queue bounds")
                    .try_reserve(driver.region_token())
                    .expect("refunded pair")
                    .cancel();
                drop(owners);
                assert_eq!(bytes.available(), 2);
            }
        },
    );
}

#[test]
fn dropped_grant_wakes() {
    with_pool::<3>(
        egress::Config::shared(egress::Credit::fixed::<1, 0>()),
        |pool, [a, b, c], driver, turn| {
            let [left, right] = [b, c].map(|key| {
                driver
                    .driver_ref()
                    .ready()
                    .make_slot(key.target().dispatch())
                    .expect("ready slot")
            });
            assert!(matches!(
                pool.egress_mut(a)
                    .expect("connection")
                    .queue
                    .try_enqueue(driver.region_token(), Either::Left(b"x")),
                egress::Enqueue::Committed
            ));
            let bounds = Bounds::one(Limit::at_most::<1, 0>());
            for (key, ready) in [(b, &left), (c, &right)] {
                assert!(
                    pool.egress_mut(key)
                        .expect("connection")
                        .queue
                        .plan(bounds)
                        .expect("bounds")
                        .poll_reserve(driver.region_token(), ready.target())
                        .is_pending()
                );
            }
            assert!(matches!(release_one(pool, a, driver, turn), Ok(true)));
            pool.credit_step(driver.region_token(), turn.maint());
            assert!(matches!(left.take(), Readiness::Ready));
            assert!(matches!(right.take(), Readiness::Idle));
            {
                let queue = pool.egress_mut(b).expect("connection").queue;
                let grant = match queue
                    .plan(bounds)
                    .expect("bounds")
                    .poll_reserve(driver.region_token(), left.target())
                {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                };
                drop(grant.expect("awakened reservation").expect("credit grant"));
            }
            pool.credit_step(driver.region_token(), turn.maint());
            assert!(matches!(right.take(), Readiness::Ready));
            assert!(matches!(
                pool.egress_mut(c)
                    .expect("connection")
                    .queue
                    .plan(bounds)
                    .expect("bounds")
                    .poll_reserve(driver.region_token(), right.target()),
                Poll::Ready(Ok(_))
            ));
        },
    );
}

#[test]
fn stale_refund_resumes() {
    with_pool::<4>(
        egress::Config::shared(egress::Credit::fixed::<2, 0>()),
        |pool, [a, b, c, d], driver, turn| {
            let [stale, left, right] = [b, c, d].map(|key| {
                driver
                    .driver_ref()
                    .ready()
                    .make_slot(key.target().dispatch())
                    .expect("ready slot")
            });
            for _ in 0..2 {
                assert!(matches!(
                    pool.egress_mut(a)
                        .expect("connection")
                        .queue
                        .try_enqueue(driver.region_token(), Either::Left(b"x")),
                    egress::Enqueue::Committed
                ));
            }
            let bounds = Bounds::one(Limit::at_most::<1, 0>());
            for (key, ready) in [(b, &stale), (c, &left), (d, &right)] {
                assert!(
                    pool.egress_mut(key)
                        .expect("connection")
                        .queue
                        .plan(bounds)
                        .expect("bounds")
                        .poll_reserve(driver.region_token(), ready.target())
                        .is_pending()
                );
            }
            drop(stale);
            assert!(matches!(release_one(pool, a, driver, turn), Ok(true)));
            assert!(matches!(release_one(pool, a, driver, turn), Ok(true)));

            let work = turn.maint();
            work.share::<{ schedule::LIMIT }, _>(|| {
                pool.credit_step(driver.region_token(), work);
                assert_eq!(work.remaining(), 0);
            });
            assert!(matches!(left.take(), Readiness::Idle));
            assert!(matches!(right.take(), Readiness::Idle));
            assert!(pool.inspect().has_credit());

            pool.credit_step(driver.region_token(), work);
            assert!(matches!(left.take(), Readiness::Ready));
            assert!(matches!(right.take(), Readiness::Ready));
            assert!(!pool.inspect().has_credit());
            for (key, ready) in [(c, &left), (d, &right)] {
                let grant = match pool
                    .egress_mut(key)
                    .expect("connection")
                    .queue
                    .plan(bounds)
                    .expect("bounds")
                    .poll_reserve(driver.region_token(), ready.target())
                {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                };
                grant
                    .expect("awakened credit is ready")
                    .expect("credit reservation")
                    .cancel();
            }
        },
    );
}

#[test]
fn protected_entry_wake() {
    with_pool::<3>(egress::Config::DEFAULT, |pool, [a, b, c], driver, turn| {
        let bytes = pool::Pool::try_new(1, 500_000).expect("payload pool");
        let ready = driver
            .driver_ref()
            .ready()
            .make_slot(b.target().dispatch())
            .expect("ready slot");
        {
            let queue = pool.egress_mut(a).expect("connection").queue;
            for _ in 0..2731 {
                assert!(matches!(
                    queue.try_enqueue(driver.region_token(), Either::Left(b"x")),
                    egress::Enqueue::Committed
                ));
            }
        }
        assert!(matches!(
            pool.egress_mut(c).expect("connection").queue.try_enqueue(
                driver.region_token(),
                Either::Right(bytes.try_cursor(b"x").expect("payload")),
            ),
            egress::Enqueue::Committed
        ));
        let bounds = Bounds::one(Limit::at_most::<1, 500_000>());
        assert!(
            pool.egress_mut(b)
                .expect("connection")
                .queue
                .plan(bounds)
                .expect("bounds")
                .poll_reserve(driver.region_token(), ready.target())
                .is_pending()
        );
        assert!(matches!(ready.take(), Readiness::Idle));

        let amount = egress::Credit::fixed::<1, 0>();
        for (key, expected) in [
            (
                b,
                egress::Admission::Busy {
                    lane: Lane::from_slot(b.lane()),
                    requested: amount,
                },
            ),
            (
                a,
                egress::Admission::Queued {
                    lane: Lane::from_slot(a.lane()),
                    requested: amount,
                    demand: amount,
                },
            ),
        ] {
            assert!(matches!(
                pool.egress_mut(key)
                    .expect("connection")
                    .queue
                    .try_enqueue(driver.region_token(), Either::Left(b"x")),
                egress::Enqueue::Rejected(rejected) if matches!(
                    rejected.error(),
                    egress::Rejection::Capacity { cause, .. } if *cause == expected
                )
            ));
        }
        assert!(matches!(
            pool.egress_mut(c)
                .expect("connection")
                .queue
                .try_enqueue(driver.region_token(), Either::Left(b"x")),
            egress::Enqueue::Committed
        ));

        assert!(matches!(release_one(pool, c, driver, turn), Ok(true)));
        pool.credit_step(driver.region_token(), turn.maint());
        assert!(matches!(ready.take(), Readiness::Ready));
        let grant = match pool
            .egress_mut(b)
            .expect("connection")
            .queue
            .plan(bounds)
            .expect("bounds")
            .poll_reserve(driver.region_token(), ready.target())
        {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        };
        grant
            .expect("awakened credit is ready")
            .expect("credit reservation")
            .cancel();
    });
}
