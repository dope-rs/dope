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
    let drops = Drops::new();

    region::Token::scope(|mut token| {
        Fixture::Queue(&drops)
            .try_with(token.proof(), |state| {
                let state = &state;
                let arena = &state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO);
                let lane = (lane).expect("lane exists");
                let mut reply = recv::Response::<_, Never>::new();
                assert!((reply.try_attach(&mut token, lane)) == (Ok(())));
                for _ in 0..4 {
                    assert!(
                        lane.select(&mut token)
                            .try_push(Counted::data(&drops))
                            .is_ok()
                    );
                }

                let mut built = false;
                assert!(
                    lane.select(&mut token)
                        .try_build(Probe {
                            built: &mut built,
                            drops: &drops,
                        },)
                        .is_err()
                );
                assert!(!built);
            })
            .expect("reply storage construction");
    });

    assert_eq!(drops.count(), 4);
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
