use dope::core::driver::{life::recovery, schedule};
use dope_fiber::reply::{
    Arena, LaneIndex, Retirement, Work, layout, recv, recv::attach::Attach as _,
};

pub mod common;
use common::recover;
use common::{
    runtime::{self, Setup, profile},
    schedule::tasks::fixtures::Rounds as _,
    storage::replies::{
        TakeBatch, TakeOne,
        support::{Cause, Counted, Drops, Never, capacity, limit},
    },
};

#[test]
fn retire_lanes_fairly() {
    let drops = Drops::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            let (_, mut driver, mut control) = scope.split();
            let layout = layout::Layout::new(capacity(2, 2, 10, 10, 10), limit(8, 8, 8))
                .expect("reply layout");
            Arena::try_batches(layout, driver.region_token().proof(), |arena, batches| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("batch lane");
                let other = arena.try_lane(LaneIndex::new::<1>()).expect("other lane");
                let mut source = recv::Response::<_, TakeBatch>::with(batches);
                source
                    .try_attach(driver.region_token(), lane)
                    .expect("batch attachment");
                for _ in 0..8 {
                    assert!(
                        lane.select(driver.region_token())
                            .try_push(Counted::mixed(&drops))
                            .is_ok()
                    );
                }
                lane.complete(driver.region_token());
                let batch = source
                    .try_take(driver.region_token())
                    .expect("completed batch must be ready")
                    .expect("batch retention");
                let mut pending = recv::Response::<_, Never>::new();
                pending
                    .try_attach(driver.region_token(), other)
                    .expect("other attachment");
                drop(pending);
                other.complete(driver.region_token());
                drop(batch);
                let mut released = false;
                for _ in 0..6 {
                    let before = drops.count();
                    let turn = control.begin();
                    turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        if let Some(lane) = arena.maintain(share.work(), driver.region_token()) {
                            released |= lane.index() == LaneIndex::new::<1>();
                        }
                    });
                    assert!(drops.count() - before <= 1);
                    turn.ready_round(|_| {});
                    if released {
                        break;
                    }
                }
                assert!(released);
                assert!(drops.count() < 8);
            })
            .expect("batch arena");
        },
        recovery::Policy::new(|_| {}, recover),
        runtime::check_scope,
    );
}

#[test]
fn retained_order_release() {
    let drops = Drops::new();
    Setup::for_profile::<profile::Tasks>().with_scope(
        |scope| {
            {
                let (_, context, mut control) = scope.split();
                let mut driver = context;
                let layout =
                    layout::Layout::bounded(capacity(1, 1, 1, 1, 1)).expect("reply layout");
                for (failed, settled, zero) in [
                    (false, false, false),
                    (true, false, false),
                    (false, true, false),
                    (true, true, false),
                    (false, false, true),
                ] {
                    Arena::try_with(layout, driver.region_token().proof(), |arena| {
                        let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                        let mut source = recv::Response::<_, TakeOne>::new();
                        source
                            .try_attach(driver.region_token(), lane)
                            .expect("reply attachment");
                        assert!(
                            lane.select(driver.region_token())
                                .try_push(if zero {
                                    Counted::from(&Cause::new(7, &drops))
                                } else {
                                    Counted::mixed(&drops)
                                })
                                .is_ok()
                        );
                        let mut row = Some(
                            source
                                .try_take(driver.region_token())
                                .expect("available row")
                                .expect("row lease"),
                        );
                        let turn = control.begin();
                        turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                            assert!(
                                arena
                                    .maintain(share.work(), driver.region_token())
                                    .is_none()
                            );
                        });
                        turn.ready_round(|_| {});
                        assert!(matches!(arena.work(), Work::Idle));
                        assert_eq!(arena.inflight().get(), 1);
                        if settled {
                            drop(row.take());
                            let turn = control.begin();
                            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                assert!(
                                    arena
                                        .maintain(share.work(), driver.region_token())
                                        .is_none()
                                );
                            });
                            turn.ready_round(|_| {});
                        }
                        if failed {
                            let turn = control.begin();
                            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                let progress = Retirement::new(driver.region_token()).fail(
                                    share.work(),
                                    lane,
                                    &Cause::new(7, &drops),
                                );
                                assert_eq!(progress.depth().get(), 0);
                            });
                            turn.ready_round(|_| {});
                        } else {
                            assert_eq!(lane.complete(driver.region_token()).get(), 0);
                        }
                        if !zero {
                            drop(row.take());
                        }
                        for _ in 0..3 {
                            if arena.inflight().get() == 0 {
                                break;
                            }
                            let turn = control.begin();
                            turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                arena.maintain(share.work(), driver.region_token());
                            });
                            turn.ready_round(|_| {});
                        }
                        assert_eq!(arena.inflight().get(), 0);
                        assert!(
                            matches!(arena.work(), Work::Idle),
                            "released entry must not leave a second retirement queued"
                        );
                        drop(row);
                    })
                    .expect("reply arena");
                }
            };
        },
        recovery::Policy::new(|_| {}, recover),
        runtime::check_scope,
    );
}
