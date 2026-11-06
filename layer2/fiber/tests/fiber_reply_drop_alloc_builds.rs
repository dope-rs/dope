use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_fiber::reply::{LaneIndex, usage::PushError};
use dope_test::{alloc, harness::scenario};

use crate::common::runtime::{self, Setup};
pub mod common;
use common::recover;

use dope_core::io::Reclaimed;
use dope_fiber::{
    reply::{recv, recv::attach::Attach as _},
    task::done::Once,
};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

use crate::common::storage::replies::{
    builds::*,
    support::{Counted, Drain, Drops, Fixture, Maintenance, Never, Releases},
};

#[global_allocator]
static ALLOCATOR: alloc::Tracker = Tracker::new();

#[test]
fn register_allocates_none() {
    let drops = Drops::new();
    let mut allocs = None;

    region::Token::scope(|mut token| {
        Fixture::Queue(&drops)
            .try_with(token.proof(), |state| {
                let state = &state;
                let arena = &state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO);
                let lane = (lane).expect("lane exists");
                let measure = Tracker::begin();
                {
                    let mut replies = [const { recv::Response::<_, Never>::new() }; 4];
                    for reply in &mut replies {
                        assert_eq!(reply.try_attach(&mut token, lane), Ok(()));
                    }
                    replies.into_iter().rev().for_each(drop);
                }
                allocs = Some(measure.finish().allocations);
            })
            .expect("reply storage construction");
    });

    assert_eq!(allocs, Some(0));
}

#[test]
fn reject_skips_build() {
    use dope_fiber::reply::usage;

    use crate::common::storage::replies::support::Collected;

    for collected in [false, true] {
        let drops = Drops::new();
        region::Token::scope(|mut token| {
            Fixture::Queue(&drops)
                .try_with(token.proof(), |state| {
                    let lane = state.arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                    let mut stream = recv::Response::<_, Never>::new();
                    let mut collect = recv::Response::<_, Collected>::new();
                    if collected {
                        collect.try_attach(&mut token, lane)
                    } else {
                        stream.try_attach(&mut token, lane)
                    }
                    .expect("reply attachment");
                    for index in 0..4 {
                        let value = Counted {
                            failure: None,
                            bytes: usize::from(index == 0),
                            drops: &drops,
                        };
                        assert!(lane.select(&mut token).try_push(value).is_ok());
                    }

                    let mut built = false;
                    let result = lane.select(&mut token).try_build(Probe {
                        built: &mut built,
                        drops: &drops,
                    });
                    let builder = match (collected, result) {
                        (false, Err(PushError::Backpressure { builder, cause })) => {
                            assert_eq!(
                                cause.into_parts(),
                                (
                                    usage::Resource::Items,
                                    usage::Required::new(1),
                                    usage::Available::new(0),
                                )
                            );
                            builder
                        }
                        (true, Err(PushError::Capacity { builder, cause })) => {
                            assert_eq!(
                                cause,
                                usage::Capacity::Limit {
                                    resource: usage::Resource::Items,
                                    current: usage::Current::new(4),
                                    added: usage::Added::new(1),
                                    limit: usage::Limit::new(4),
                                }
                            );
                            builder
                        }
                        _ => panic!("full reply"),
                    };
                    assert!(!*builder.built);
                    if !collected {
                        for (bytes, expected) in [
                            (
                                2,
                                usage::Capacity::Limit {
                                    resource: usage::Resource::Bytes,
                                    current: usage::Current::new(1),
                                    added: usage::Added::new(2),
                                    limit: usage::Limit::new(1),
                                },
                            ),
                            (
                                usize::MAX,
                                usage::Capacity::Arithmetic {
                                    resource: usage::Resource::Bytes,
                                    current: usage::Current::new(1),
                                    added: usage::Added::new(usize::MAX),
                                },
                            ),
                        ] {
                            let value = Counted {
                                failure: None,
                                bytes,
                                drops: &drops,
                            };
                            let value = match lane.select(&mut token).try_push(value) {
                                Err(usage::InsertError::Capacity { value, cause }) => {
                                    assert_eq!(cause, expected);
                                    value
                                }
                                _ => panic!("permanent rejection"),
                            };
                            assert_eq!(value.bytes, bytes);
                        }
                    }
                })
                .expect("reply storage construction");
        });

        assert_eq!(drops.count(), if collected { 4 } else { 6 });
    }
}

#[test]
fn build_failure_rolls_back() {
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
                    let lane = arena.try_lane(LaneIndex::ZERO);
                    let lane = (lane).expect("lane exists");
                    {
                        let mut failed = recv::Response::<_, Never>::new();
                        assert!(
                            (failed.try_attach(session.driver().region_token(), lane)) == (Ok(()))
                        );
                        assert!(matches!(
                            lane.select(session.driver().region_token()).try_build(Fail),
                            Err(PushError::Build { error: Reject, .. })
                        ));
                        lane.complete(session.driver().region_token());
                    }
                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(matches!(result, Ok(Ok(()))), "finalize reply maintenance");
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));

                    let mut next = recv::Response::<_, Never>::new();
                    assert!((next.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    for _ in 0..4 {
                        assert!(
                            lane.select(session.driver().region_token())
                                .try_push(Counted::data(&drops))
                                .is_ok(),
                            "build failure must return every global credit"
                        );
                    }
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 4);
}

#[test]
fn first_retains_until_terminal() {
    use dope_fiber::reply::{Arena, layout, order::Front, usage};

    use crate::common::storage::replies::support::{Cause, Error, RetainOne, limit};

    for failed in [false, true] {
        let drops = Drops::new();
        let failures = Drops::new();
        region::Token::scope(|mut token| {
            let layout = layout::Layout::single(layout::Entries::new(1), limit(1, 1, 1))
                .expect("reply layout");
            Arena::try_with(layout, token.proof(), |arena| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                let mut reply = recv::Response::<_, RetainOne<true>>::new();
                assert!(reply.try_attach(&mut token, lane).is_ok());
                let reserve = lane
                    .try_reserve(usage::Usage::new(usage::Bytes::ZERO, usage::Values::new(1)))
                    .expect("reserve value credit");
                assert!(matches!(
                    lane.select(&mut token).try_build(Fail),
                    Err(PushError::Capacity {
                        cause: usage::Capacity::Unavailable(_),
                        ..
                    })
                ));
                drop(reserve);
                assert!(matches!(
                    lane.select(&mut token).try_build(Fail),
                    Err(PushError::Build { error: Reject, .. })
                ));
                assert!(
                    lane.select(&mut token)
                        .try_push(Counted::mixed(&drops))
                        .is_ok()
                );
                let mut built = false;
                assert!(
                    lane.select(&mut token)
                        .try_build(Probe {
                            built: &mut built,
                            drops: &drops,
                        })
                        .is_ok()
                );
                assert!(!built);
                assert!(
                    lane.select(&mut token)
                        .try_push(Counted::data(&drops))
                        .is_ok()
                );
                assert_eq!(drops.count(), 1);
                assert!(matches!(lane.select(&mut token).front(), Front::Open));
                assert!(reply.try_take(&mut token).is_none());
                if failed {
                    assert!(
                        lane.select(&mut token)
                            .try_fail(Counted::from(&Cause::new(7, &failures)))
                            .is_ok()
                    );
                } else {
                    lane.complete(&mut token);
                }
                let output = reply.try_take(&mut token).expect("terminal reply");
                if failed {
                    assert!(matches!(output, Err(Error::Failed(Some(7)))));
                } else {
                    let (first, _credits) = output.expect("first retained item").into_parts();
                    assert_eq!(first.bytes, 1);
                    assert_eq!(drops.count(), 1);
                }
            })
            .expect("reply arena");
        });
        assert_eq!(drops.count(), 2);
        assert_eq!(failures.count(), usize::from(failed));
    }
}
