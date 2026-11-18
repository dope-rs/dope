use std::{convert, pin::Pin, task::Poll};

use dope::core::driver::{
    life::recovery,
    route::{Epoch, KeyTag, indices::SlotIndex},
};
use dope_fiber::{
    context::Context,
    reply::{Arena, LaneIndex, Work, layout, recv, recv::attach::Attach as _, usage},
};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

pub mod common;
use common::{
    recover,
    runtime::{self, Setup},
    storage::replies::{
        builds::{SCALAR, ScalarBuild},
        support::{Cause, Counted, Drops, Finish, capacity},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn terminal_poll_retains_result_and_exact_error() {
    let values = Drops::new();
    let failures = Drops::new();
    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = site.ready().make_slot(target).expect("reply wake slot");
            let layout = layout::Layout::bounded(capacity(1, 4, 3, 3 * SCALAR.len(), 3))
                .expect("reply layout");
            Arena::<_, Counted<'_>>::try_with(layout, driver.region_token().proof(), |arena| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                let mut response = recv::Response::<_, recv::Collected, Counted<'_>>::new();
                response
                    .try_attach(driver.region_token(), lane)
                    .expect("reply attachment");
                Context::with_target(ready.target(), turn.turn().app(), driver.reborrow(), |cx| {
                    assert!(
                        Pin::new(&mut response)
                            .poll_all(cx, convert::identity)
                            .is_pending()
                    );
                });
                assert!(turn.ready().drain(|_| {}).is_none());
                assert!(
                    lane.select(driver.region_token())
                        .try_build(ScalarBuild {
                            bytes: &SCALAR,
                            drops: &values,
                        })
                        .is_ok()
                );
                assert!(turn.ready().drain(|_| {}).is_none());
                lane.complete(driver.region_token());
                assert!(turn.ready().drain(|_| {}).is_some());
                let batch = Context::with_target(
                    ready.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |cx| {
                        let Poll::Ready(result) =
                            Pin::new(&mut response).poll_all(cx, convert::identity)
                        else {
                            unreachable!("completed")
                        };
                        result
                            .map_err(|error| error.failure)
                            .expect("completed reply")
                    },
                );
                drop(response);
                assert!(matches!(arena.work(), Work::Idle));
                assert_eq!(batch.iter().next().expect("retained row").bytes, SCALAR);

                let mut empty = recv::Response::<_, recv::First, Counted<'_>>::new();
                empty
                    .try_attach(driver.region_token(), lane)
                    .expect("empty attachment");
                lane.complete(driver.region_token());
                Context::with_target(ready.target(), turn.turn().app(), driver.reborrow(), |cx| {
                    assert!(matches!(
                        Pin::new(&mut empty).poll_first(cx, convert::identity),
                        Poll::Ready(Ok(None))
                    ));
                });
                let mut failed = recv::Response::<_, recv::First, Counted<'_>>::new();
                failed
                    .try_attach(driver.region_token(), lane)
                    .expect("failed attachment");
                Context::with_target(ready.target(), turn.turn().app(), driver.reborrow(), |cx| {
                    assert!(
                        Pin::new(&mut failed)
                            .poll_first(cx, convert::identity)
                            .is_pending()
                    );
                });
                assert!(
                    lane.select(driver.region_token())
                        .try_build(ScalarBuild {
                            bytes: &SCALAR,
                            drops: &values,
                        })
                        .is_ok()
                );
                assert!(turn.ready().drain(|_| {}).is_none());
                assert!(
                    lane.select(driver.region_token())
                        .try_fail(Counted::from(&Cause::new(17, &failures)))
                        .is_ok()
                );
                assert!(turn.ready().drain(|_| {}).is_some());
                Context::with_target(ready.target(), turn.turn().app(), driver.reborrow(), |cx| {
                    assert!(matches!(
                        Pin::new(&mut failed)
                            .poll_first(cx, |result| result.map_err(|error| error.failure)),
                        Poll::Ready(Err(Some(17)))
                    ));
                });
                assert_eq!(values.count(), 0);
                drop(batch);

                let mut ordinary = recv::Response::<_, Finish, Counted<'_>>::new();
                ordinary
                    .try_attach(driver.region_token(), lane)
                    .expect("ordinary collected attachment");
                Context::with_target(ready.target(), turn.turn().app(), driver.reborrow(), |cx| {
                    assert!(Pin::new(&mut ordinary).poll_next(cx).is_pending());
                });
                assert!(
                    lane.select(driver.region_token())
                        .try_build(ScalarBuild {
                            bytes: &SCALAR,
                            drops: &values,
                        })
                        .is_ok()
                );
                assert!(turn.ready().drain(|_| {}).is_some());
            })
            .expect("reply arena");
        },
        recovery::Policy::new(|_| {}, recover),
        runtime::check_scope,
    );
    assert_eq!((values.count(), failures.count()), (3, 1));
}

#[test]
fn first_transfers_root_and_retained_credit() {
    let drops = Drops::new();
    let layout = layout::Layout::bounded(capacity(1, 1, 1, SCALAR.len(), 1)).expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::<_, Counted<'_>>::try_with(layout, token.proof(), |arena| {
            let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
            let mut response = recv::Response::<_, recv::First, Counted<'_>>::new();
            response
                .try_attach(&mut token, lane)
                .expect("reply attachment");
            assert!(
                lane.select(&mut token)
                    .try_build(ScalarBuild {
                        bytes: &SCALAR,
                        drops: &drops,
                    })
                    .is_ok()
            );
            lane.complete(&mut token);
            let (row, activity) = Tracker::activity(|| response.try_first(&mut token));
            let row = row
                .expect("terminal reply")
                .map_err(|error| error.failure)
                .expect("successful reply")
                .expect("first row");
            drop(response);
            assert_eq!(activity.allocations, 0);
            assert_eq!(row.get().bytes, SCALAR);
            assert!(matches!(arena.work(), Work::Idle));
            assert_eq!(arena.inflight().get(), 1);
            let amount = usage::Usage::new(usage::Bytes::new(SCALAR.len()), usage::Values::new(1));
            assert!(lane.try_reserve(amount).is_err());
            drop(row);
            assert_eq!(drops.count(), 1);
            assert!(matches!(arena.work(), Work::Pending));
            drop(lane.try_reserve(amount).expect("returned retained credit"));
        })
        .expect("reply arena");
    });
}

#[test]
fn drain_retires_visited_items() {
    for stop in [2, 4] {
        let source = Drops::new();
        region::Token::scope(|mut token| {
            let layout = layout::Layout::bounded(capacity(1, 1, 4, 4, 4)).expect("reply layout");
            Arena::try_with(layout, token.proof(), |arena| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                let mut reply = recv::Response::<_, recv::Collected>::new();
                reply
                    .try_attach(&mut token, lane)
                    .expect("reply attachment");
                for _ in 0..4 {
                    assert!(
                        lane.select(&mut token)
                            .try_push(Counted::mixed(&source))
                            .is_ok()
                    );
                }
                lane.complete(&mut token);
                let mut batch = reply
                    .try_all(&mut token)
                    .expect("completed batch")
                    .map_err(|error| error.failure)
                    .expect("batch");
                let mut visited = 0;
                let result = batch.try_drain(|row| {
                    assert_eq!(source.count(), visited);
                    assert_eq!(row.bytes, 1);
                    visited += 1;
                    if visited == 2 && stop == 2 {
                        return Err(7);
                    }
                    Ok(())
                });
                assert_eq!(result, if stop == 2 { Err(7) } else { Ok(()) });
                assert_eq!(source.count(), stop);
                assert_eq!(batch.len().get(), 4 - stop);
                let returned = usage::Usage::new(usage::Bytes::new(stop), usage::Values::new(stop));
                drop(lane.try_reserve(returned).expect("returned prefix credit"));
            })
            .expect("batch arena");
        });
        assert_eq!(source.count(), 4);
    }
}
