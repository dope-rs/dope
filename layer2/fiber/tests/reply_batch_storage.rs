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
fn map_retires_consumed_rows() {
    for stop in [0, 2, 4] {
        let source = Drops::new();
        let output = Drops::new();
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
                let batch = reply
                    .try_all(&mut token)
                    .expect("completed batch")
                    .map_err(|error| error.failure)
                    .expect("batch");
                let mut visited = 0;
                let result = Tracker::fail_after(usize::from(stop != 0), || {
                    batch.try_map(|row, slot| {
                        assert_eq!(source.count(), visited);
                        assert_eq!(row.bytes, 1);
                        if visited != 0 {
                            drop(
                                lane.try_reserve(usage::Usage::new(
                                    usage::Bytes::new(visited),
                                    usage::Values::new(visited),
                                ))
                                .expect("returned credit"),
                            );
                        }
                        visited += 1;
                        let written = slot.insert(Counted::mixed(&output));
                        if visited == stop && stop < 4 {
                            Err(7)
                        } else {
                            Ok(written)
                        }
                    })
                });
                assert_eq!(visited, stop);
                assert_eq!(source.count(), stop);
                match (stop, result) {
                    (0, Err(_)) | (2, Ok(Err(7))) => assert_eq!(output.count(), stop),
                    (4, Ok(Ok(values))) => {
                        assert_eq!(output.count(), 0);
                        assert_eq!(values.len(), 4);
                        drop(values);
                    }
                    _ => panic!("mapping result"),
                }
            })
            .expect("batch arena");
        });
        assert_eq!(source.count(), 4);
        assert_eq!(output.count(), stop);
    }
}
