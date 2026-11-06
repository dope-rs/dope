use dope_fiber::reply::{Arena, LaneIndex, layout, order, recv, recv::attach::Attach as _};
use o3::base::cell::region;

pub mod common;
use common::storage::replies::support::{Cause, Counted, Drops, Never, RetainOne, capacity};

#[test]
fn retains_rows_after_tags() {
    let tags = Drops::new();
    let rows = Drops::new();
    let layout = layout::Layout::bounded(capacity(2, 4, 3, 3, 3)).expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::try_with(layout, token.proof(), |arena| {
            let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
            let other = arena.try_lane(LaneIndex::new::<1>()).expect("other lane");
            let mut done = recv::Response::<_, RetainOne, _, _>::new();
            done.try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("done reply");
            let cutoff = lane.cutoff(&token);
            let mut failed = recv::Response::<_, Never, _, _>::new();
            failed
                .try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("failed reply");
            assert!(lane.contains_cutoff(cutoff, &token));
            assert!(!other.contains_cutoff(cutoff, &token));
            assert!(cutoff != lane.cutoff(&token));
            assert!(lane.select(&mut token).is_cutoff(cutoff));
            assert!(
                lane.select(&mut token)
                    .try_push(Counted::mixed(&rows))
                    .is_ok()
            );
            lane.complete(&mut token);
            assert_eq!(tags.count(), 1);
            let row = done
                .try_take(&mut token)
                .expect("done output")
                .expect("retained row");
            assert!(!lane.contains_cutoff(cutoff, &token));
            assert_eq!(done.detach_order(&token), order::State::Detached);
            drop(done);
            assert_eq!(rows.count(), 0);

            assert!(
                lane.select(&mut token)
                    .try_fail(Counted::from(&Cause::new(7, &rows)))
                    .is_ok()
            );
            assert_eq!(tags.count(), 2);
            assert_eq!(failed.detach_order(&token), order::State::Detached);
            let mut pending = recv::Stream::<_, Never, _, _>::new();
            pending
                .try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("pending reply");
            let cutoff = lane.cutoff(&token);
            assert_eq!(pending.detach_order(&token), order::State::Linked);
            assert_eq!(pending.detach_order(&token), order::State::Detached);
            assert!(lane.contains_cutoff(cutoff, &token));
            assert!(lane.select(&mut token).is_cutoff(cutoff));
            drop(row);
            assert_eq!(rows.count(), 1);
        })
        .expect("reply arena");
    });
    assert_eq!(tags.count(), 3);
    assert_eq!(rows.count(), 2);
}
