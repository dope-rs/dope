use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_fiber::reply::{Arena, LaneIndex, Work, layout::Layout, recv::attach::Error};
use dope_test::{alloc, harness::scenario};

use crate::common::runtime::{self, Empty, Setup};
pub mod common;

use std::{convert::Infallible, mem};

use dope_core::io::Reclaimed;
use dope_fiber::{reply::recv, task::done::Once};
use dope_test::alloc::Tracker;
use o3::base::cell::region;
use recv::attach::Attach as _;
use support::{Counted, Drain, Drops, Fixture, Maintenance, Never, Releases};

use crate::common::storage::replies::*;

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}

#[test]
fn drop_allocates_none() {
    let drops = Drops::new();
    let mut measure = None;

    region::Token::scope(|mut token| {
        Fixture::Queue(&drops)
            .try_with(token.proof(), |state| {
                let state = &state;
                let arena = &state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO);
                let lane = (lane).expect("lane exists");
                {
                    let mut reply = recv::Response::<_, Never>::new();
                    assert!((reply.try_attach(&mut token, lane)) == (Ok(())));
                    for _ in 0..4 {
                        assert!(
                            lane.select(&mut token)
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                    reply.detach();
                    assert!((arena.work()) == (Work::Pending));
                }
                measure = Some(Tracker::begin());
            })
            .expect("reply storage construction");
    });

    let measure = (measure).expect("allocation measurement must start");
    let allocs = measure.finish().allocations;

    assert_eq!(allocs, 0);
    assert_eq!(drops.count(), 4);
}

#[test]
fn mixed_lanes_release() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Mixed(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let left = arena.try_lane(LaneIndex::ZERO).expect("left lane");
                    let right = arena.try_lane(LaneIndex::new::<1>()).expect("right lane");
                    let mut left_rx = recv::Response::<_, RetainMany>::new();
                    let mut right_rx = recv::Response::<_, RetainMany>::new();
                    assert!(
                        left_rx
                            .try_attach(session.driver().region_token(), left)
                            .is_ok()
                    );
                    assert!(
                        right_rx
                            .try_attach(session.driver().region_token(), right)
                            .is_ok()
                    );
                    for _ in 0..3 {
                        assert!(
                            left.select(session.driver().region_token())
                                .try_push(Counted::mixed(&drops))
                                .is_ok()
                        );
                        assert!(
                            right
                                .select(session.driver().region_token())
                                .try_push(Counted::mixed(&drops))
                                .is_ok()
                        );
                    }
                    left.complete(session.driver().region_token());
                    right.complete(session.driver().region_token());
                    let releases = Releases::new();
                    {
                        let cloned = {
                            let outcome = session.with_quiesce(
                                &mut finish,
                                Empty {},
                                scenario::TIMEOUT,
                                |mut app| app.drive(Once::new(left_rx)),
                            );
                            assert!(
                                outcome.is_ok(),
                                "finalize left app: {:?}",
                                outcome.as_ref().err().map(|error| error.cause())
                            );
                            let left_set = outcome
                                .ok()
                                .expect("finalize left app")
                                .expect("retain left reply")
                                .expect("reply retention");
                            let outcome = session.with_quiesce(
                                &mut finish,
                                Empty {},
                                scenario::TIMEOUT,
                                |mut app| app.drive(Once::new(right_rx)),
                            );
                            assert!(
                                outcome.is_ok(),
                                "finalize right app: {:?}",
                                outcome.as_ref().err().map(|error| error.cause())
                            );
                            let rightset = outcome
                                .ok()
                                .expect("finalize right app")
                                .expect("retain right reply")
                                .expect("reply retention");
                            let left_one = left_set.0.into_parts();
                            let leftheld = left_one.1.into_parts();
                            let left_buf = leftheld.0.expect("left byte component");
                            let rightone = rightset.0.into_parts();
                            let rtheld = rightone.1.into_parts();
                            assert!(rtheld.0.is_some());
                            assert!(leftheld.1.is_some());
                            assert!(rtheld.1.is_some());
                            left_buf.try_clone()
                        };
                        assert!(cloned.is_ok(), "clone left bytes");
                        let result = session.with_quiesce(
                            &mut finish,
                            Maintenance::new(state, &releases),
                            scenario::TIMEOUT,
                            |mut app| app.drive(Once::new(Drain::new(state))),
                        );
                        assert!(matches!(result, Ok(Ok(()))));
                        assert_eq!(
                            arena.inflight().get(),
                            1,
                            "the byte clone keeps only its own lane alive"
                        );
                        let released = releases.take();
                        assert!(released.into_iter().map(|lane| lane.index().get()).eq([1]));
                    }
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(matches!(result, Ok(Ok(()))));
                    assert_eq!(arena.inflight().get(), 0);
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));
                    let mut next_l = recv::Response::<_, Never>::new();
                    let mut next_r = recv::Response::<_, Never>::new();
                    assert!(
                        next_l
                            .try_attach(session.driver().region_token(), left)
                            .is_ok()
                    );
                    assert!(
                        next_r
                            .try_attach(session.driver().region_token(), right)
                            .is_ok()
                    );
                    for _ in 0..3 {
                        assert!(
                            left.select(session.driver().region_token())
                                .try_push(Counted::mixed(&drops))
                                .is_ok()
                        );
                        assert!(
                            right
                                .select(session.driver().region_token())
                                .try_push(Counted::mixed(&drops))
                                .is_ok()
                        );
                    }
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 12);
}

#[test]
fn scope_owns_arena() {
    let drops = Drops::new();

    region::Token::scope(|mut token| {
        Fixture::Queue(&drops)
            .try_with(token.proof(), |state| {
                let arena = state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO).expect("first lane");
                {
                    let mut response = recv::Response::<_, Never>::new();
                    response
                        .try_attach(&mut token, lane)
                        .expect("reply attachment");
                    for _ in 0..2 {
                        assert!(
                            lane.select(&mut token)
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                }
                assert_eq!(drops.count(), 0);
            })
            .expect("reply storage construction");
        assert_eq!(drops.count(), 2);
    });
}

#[test]
fn owner_holds_credits() {
    for forget in [false, true] {
        let mut finish = Completion::new();
        let drops = Drops::new();
        let mut reports = Vec::with_capacity(1);

        Setup::throughput().with_session(
            |mut session| {
                let proof = session.driver().region_token().proof();
                let storage = if forget {
                    Fixture::Slot(&drops)
                } else {
                    Fixture::Queue(&drops)
                };
                storage
                    .try_with(proof, |state| {
                        let state = &state;
                        let arena = &state.arena;
                        let lane = arena.try_lane(LaneIndex::ZERO);
                        let lane = (lane).expect("lane exists");
                        let mut response = recv::Response::<_, RetainMany>::new();
                        assert!(
                            (response.try_attach(session.driver().region_token(), lane))
                                == (Ok(()))
                        );
                        for index in 0..3 {
                            let value = if forget && index == 0 {
                                Counted::mixed(&drops)
                            } else {
                                Counted::data(&drops)
                            };
                            assert!(
                                lane.select(session.driver().region_token())
                                    .try_push(value)
                                    .is_ok()
                            );
                        }
                        lane.complete(session.driver().region_token());

                        let releases = Releases::new();
                        {
                            let outcome = session.with_quiesce(
                                &mut finish,
                                Empty {},
                                scenario::TIMEOUT,
                                |mut app| app.drive(Once::new(response)),
                            );
                            assert!(
                                outcome.is_ok(),
                                "finalize test app: {:?}",
                                outcome.as_ref().err().map(|error| error.cause())
                            );
                            let retained = outcome
                                .ok()
                                .expect("finalize test app")
                                .expect("receive retained response")
                                .expect("reply retention");
                            assert!(retained.1.get().is_data());
                            assert_eq!(arena.inflight().get(), 1);
                            let result = session.with_quiesce(
                                &mut finish,
                                Maintenance::new(state, &releases),
                                scenario::TIMEOUT,
                                |mut app| app.drive(Once::new(Drain::new(state))),
                            );
                            assert!(
                                matches!(result, Ok(Ok(()))),
                                "finalize retained entry retirement"
                            );
                            assert!((arena.work()) == (Work::Idle));
                            assert!(releases.take().is_empty());
                            if forget {
                                let (head, tail, leased) = {
                                    let (first, leased, last) = retained;
                                    let first = first.into_parts();
                                    let last = last.into_parts();
                                    (first.1, last.1, leased)
                                };
                                mem::forget((head, tail, leased));
                                assert_eq!(drops.count(), 2);
                                assert_eq!(arena.inflight().get(), 1);
                                let mut blocked = recv::Response::<_, Never>::new();
                                assert!(matches!(
                                    blocked.try_attach(session.driver().region_token(), lane),
                                    Err(Error::Full { len, capacity })
                                        if len.get() == 1 && capacity.get() == 1
                                ));
                                return;
                            }
                        }
                        assert!((arena.work()) == (Work::Pending));
                        assert_eq!(arena.inflight().get(), 1);
                        let result = session.with_quiesce(
                            &mut finish,
                            Maintenance::new(state, &releases),
                            scenario::TIMEOUT,
                            |mut app| app.drive(Once::new(Drain::new(state))),
                        );
                        assert!(
                            matches!(result, Ok(Ok(()))),
                            "finalize retained reply maintenance"
                        );
                        assert_eq!(arena.inflight().get(), 0);
                        let released = releases.take();
                        assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));
                        assert!((arena.work()) == (Work::Idle));
                    })
                    .expect("reply storage construction");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
        assert!(
            matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert_eq!(drops.count(), if forget { 2 } else { 3 });
    }
}

#[test]
fn batch_drops_before_pool() {
    for forget in [false, true] {
        let mut finish = Completion::new();
        let drops = Drops::new();
        let mut measure = None;
        let mut reports = Vec::with_capacity(1);

        Setup::throughput().with_session(
            |mut session| {
                let proof = session.driver().region_token().proof();
                let layout = Layout::new(support::capacity(1, 4, 4, 1, 4), support::limit(4, 1, 4))
                    .expect("reply layout");
                Arena::try_batches(layout, proof, |arena, batches| {
                    let lane = arena.try_lane(LaneIndex::ZERO);
                    let lane = (lane).expect("lane exists");
                    let mut response = recv::Response::<_, TakeBatch>::with(batches);
                    assert!(
                        (response.try_attach(session.driver().region_token(), lane)) == (Ok(()))
                    );
                    for _ in 0..4 {
                        assert!(
                            lane.select(session.driver().region_token())
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                    lane.complete(session.driver().region_token());
                    let outcome = session.with_quiesce(
                        &mut finish,
                        Empty {},
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(response)),
                    );
                    assert!(
                        outcome.is_ok(),
                        "finalize test app: {:?}",
                        outcome.as_ref().err().map(|error| error.cause())
                    );
                    let batch = outcome
                        .ok()
                        .expect("finalize test app")
                        .expect("receive completed batch")
                        .expect("reply retention");
                    assert_eq!(batch.len().get(), 4);
                    measure = Some(Tracker::begin());
                    if forget {
                        mem::forget(batch);
                        assert_eq!(arena.inflight().get(), 1);
                        assert_eq!(drops.count(), 0);
                    }
                })
                .expect("reply storage construction");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
        assert!(
            matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
            "unexpected reclaimed operations: {reports:?}"
        );

        let measure = (measure).expect("allocation measurement must start");
        let allocs = measure.finish().allocations;

        assert_eq!(allocs, 0);
        assert_eq!(drops.count(), 4);
    }
}
