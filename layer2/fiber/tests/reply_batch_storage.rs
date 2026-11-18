use dope_fiber::reply::{Arena, LaneIndex, Work, layout, recv, recv::attach::Attach as _, usage};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

pub mod common;
use common::storage::replies::{
    builds::{SCALAR, ScalarBuild},
    support::{Counted, Drops, capacity},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

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
