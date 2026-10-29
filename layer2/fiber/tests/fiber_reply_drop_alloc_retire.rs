use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_fiber::reply::{Progress, Retirement, Work, usage};
use dope_test::{alloc, alloc::Tracker, harness::scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::convert::Infallible;

use dope_core::io::Reclaimed;
use dope_fiber::{
    reply::{
        Attachment, LaneIndex,
        order::{Depth, Front},
        recv,
        recv::attach::Attach as _,
    },
    task::done::Once,
};
use o3::base::cell::region;

use crate::common::storage::replies::{
    retire::*,
    support::{Counted, Drain, Drops, Fixture, Maintenance, Never, Releases, RetainOne},
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}

#[test]
fn retry_after_cancel() {
    let drops = Drops::new();
    let mut finish = Completion::new();
    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Queue(&drops)
                .try_with(proof, |state| {
                    let lane = state.arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                    let mut source = recv::Response::<_, RetainOne>::new();
                    source
                        .try_attach(session.driver().region_token(), lane)
                        .expect("source reply");
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::mixed(&drops))
                            .is_ok()
                    );
                    lane.complete(session.driver().region_token());
                    let held = source
                        .try_take(session.driver().region_token())
                        .expect("completed reply")
                        .expect("retained reply");

                    let mut blocked = recv::Response::<_, Never>::new();
                    blocked
                        .try_attach(session.driver().region_token(), lane)
                        .expect("blocked reply");
                    let mut next = recv::Response::<_, Never>::new();
                    next.try_attach(session.driver().region_token(), lane)
                        .expect("next reply");
                    let error = lane
                        .select(session.driver().region_token())
                        .try_push(Counted::mixed(&drops))
                        .expect_err("retained byte credit");
                    let (value, cause) = match error {
                        usage::InsertError::Backpressure { value, cause } => Some((value, cause)),
                        _ => None,
                    }
                    .expect("byte backpressure");
                    let (resource, required, avail) = cause.into_parts();
                    assert_eq!(
                        (resource, required.get(), avail.get()),
                        (usage::Resource::Bytes, 1, 0)
                    );
                    assert_eq!(drops.count(), 0);

                    drop(blocked);
                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(&state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(&state))),
                    );
                    assert!(matches!(result, Ok(Ok(()))), "retire blocked reply");
                    let error = lane
                        .select(session.driver().region_token())
                        .try_push(value)
                        .expect_err("detached reply");
                    assert!(matches!(
                        &error,
                        usage::InsertError::Front {
                            front: Front::Detached,
                            ..
                        }
                    ));
                    assert_eq!(drops.count(), 0);
                    drop(error);
                    assert_eq!(drops.count(), 1);
                    assert_eq!(lane.depth(session.driver().region_token()).get(), 2);
                    assert_eq!(lane.complete(session.driver().region_token()).get(), 1);
                    assert!(matches!(
                        lane.select(session.driver().region_token()).front(),
                        Front::Open
                    ));
                    lane.complete(session.driver().region_token());
                    drop((next, held));
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(
            |event| assert!(matches!(event, Reclaimed::Shutdown)),
            recover,
        ),
        check_scope,
    );
    assert_eq!(drops.count(), 2);
}

#[test]
fn detach_waits_for_finish() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Queue(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = arena.try_lane(LaneIndex::ZERO).expect("lane exists");
                    let mut reply = recv::Response::<_, Never>::new();
                    assert!((reply.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    assert_eq!(lane.depth(session.driver().region_token()).get(), 1);

                    reply.detach();
                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize detached reply maintenance"
                    );
                    assert!(releases.take().is_empty());
                    {
                        let mut driver = session.driver();
                        let region = driver.region_token();
                        assert!((lane.control().attachment(region)) == (Attachment::Detached));
                        assert_eq!(lane.depth(region).get(), 1);
                        assert_eq!(arena.inflight().get(), 1);

                        lane.complete(region);
                        assert!((lane.depth(region)) == (Depth::ZERO));
                        assert_eq!(arena.inflight().get(), 1);
                    }
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize detached completion reclaim"
                    );
                    assert_eq!(arena.inflight().get(), 0);
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));
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
}

#[test]
fn keeps_reply_order() {
    let drops = Drops::new();

    region::Token::scope(|mut token| {
        Fixture::Queue(&drops)
            .try_with(token.proof(), |state| {
                let state = &state;
                let arena = &state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO).expect("lane exists");
                let mut replies = [const { recv::Response::<_, Never>::new() }; 2];
                for (index, response) in replies.iter_mut().enumerate() {
                    let depth = response
                        .try_ordered(&mut token, lane, ())
                        .expect("reply registration must succeed");
                    assert_eq!(depth.get(), index + 1);
                }

                assert_eq!(lane.depth(&mut token).get(), 2);
                assert_eq!(lane.complete(&mut token).get(), 1);
                assert!((lane.select(&mut token).front()) == (Front::Open));
                assert!((lane.complete(&mut token)) == (Depth::ZERO));
                assert!((lane.select(&mut token).front()) == (Front::Empty));
                for response in &mut replies {
                    response.detach();
                }
            })
            .expect("reply storage construction");
    });
}

#[test]
fn drops_one_per_step() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Queue(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = arena.try_lane(LaneIndex::ZERO).expect("lane exists");
                    let mut reply = recv::Response::<_, Never>::new();
                    assert!((reply.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    for _ in 0..4 {
                        assert!(
                            lane.select(session.driver().region_token())
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                    lane.complete(session.driver().region_token());
                    reply.detach();

                    assert_eq!(arena.inflight().get(), 1);
                    assert_eq!(drops.count(), 0);
                    assert!((arena.work()) == (Work::Pending));
                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize bounded reply maintenance"
                    );
                    assert!(releases.max_drops() <= 1);
                    assert_eq!(arena.inflight().get(), 0);
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));
                    assert!((arena.work()) == (Work::Idle));
                    assert_eq!(drops.count(), 4);
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
}

#[test]
fn complete_during_drain() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Queue(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = arena.try_lane(LaneIndex::ZERO).expect("lane exists");
                    let mut reply = recv::Response::<_, Never>::new();
                    assert!((reply.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    for _ in 0..4 {
                        assert!(
                            lane.select(session.driver().region_token())
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                    reply.detach();

                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| {
                            let (dropped, front, depth) = app.drive(Once::new(Step {
                                drops: &drops,
                                lane,
                            }))?;
                            assert_eq!(dropped, 1);
                            assert!(front == Front::Detached);
                            assert!(depth == Depth::ZERO);
                            app.drive(Once::new(Drain::new(state)))
                        },
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finish active reply retirement"
                    );
                    assert!(releases.max_drops() <= 1);
                    assert_eq!(drops.count(), 4);
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
}

#[test]
fn partial_stays_live() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Queue(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = arena.try_lane(LaneIndex::ZERO).expect("lane exists");
                    let mut reply = recv::Response::<_, Never>::new();
                    assert!((reply.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    for _ in 0..4 {
                        assert!(
                            lane.select(session.driver().region_token())
                                .try_push(Counted::data(&drops))
                                .is_ok()
                        );
                    }
                    reply.detach();

                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize partial reply retirement"
                    );
                    assert!(
                        (lane.select(session.driver().region_token()).front()) == (Front::Detached)
                    );
                    assert_eq!(drops.count(), 4);
                    assert_eq!(arena.inflight().get(), 1);
                    assert!(releases.take().is_empty());

                    lane.complete(session.driver().region_token());
                    assert_eq!(arena.inflight().get(), 1);
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize detached reply reclaim"
                    );
                    assert_eq!(drops.count(), 4);
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
}

#[test]
fn failure_uses_budget() {
    use dope::core::driver::schedule;

    use crate::common::storage::replies::support::Cause;

    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_scope(
        |scope| {
            scope.with_driver(|mut driver, mut control| {
                Fixture::Queue(&drops)
                    .try_with(driver.region_token().proof(), |state| {
                        let lane = state.arena.try_lane(LaneIndex::ZERO);
                        let lane = (lane).expect("required fixture outcome");
                        let mut replies = [const { recv::Response::<_, Never>::new() }; 3];
                        for response in &mut replies {
                            assert!((response.try_attach(driver.region_token(), lane)) == (Ok(())));
                        }
                        let cause = Cause::new(17, &drops);
                        for expected in [2, 1, 0] {
                            let turn = control.begin();
                            let measure = Tracker::begin();
                            let progress =
                                turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                                    let progress = Retirement::new(driver.region_token()).fail(
                                        share.work(),
                                        lane,
                                        &cause,
                                    );
                                    if expected == 0 {
                                        assert_eq!(share.work().remaining(), 0);
                                        assert!(
                                            Retirement::new(driver.region_token()).fail(
                                                share.work(),
                                                lane,
                                                &cause,
                                            ) == progress
                                        );
                                    }
                                    progress
                                });
                            assert_eq!(measure.finish().allocations, 0);
                            if expected == 0 {
                                assert!(progress == Progress::Complete(Depth::ZERO));
                            } else {
                                assert!(matches!(progress, Progress::Pending(depth)
                                    if depth.get() == expected));
                            }
                            assert_eq!(drops.count(), 0);
                        }
                        let turn = control.begin();
                        assert!(
                            (Retirement::new(driver.region_token()).fail(
                                turn.turn().app(),
                                lane,
                                &cause
                            )) == (Progress::Complete(Depth::ZERO))
                        );
                    })
                    .expect("reply storage construction");
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 3);
}
