use dope_fiber::reply::{Arena, LaneIndex, layout, recv, recv::attach::Attach as _};
use o3::base::cell::region;

pub mod common;
use common::storage::replies::support::{Cause, Counted, Drops, Never, RetainOne, capacity};

#[test]
fn retains_rows_after_tags() {
    let tags = Drops::new();
    let rows = Drops::new();
    let layout = layout::Layout::bounded(capacity(1, 3, 3, 3, 3)).expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::try_with(layout, token.proof(), |arena| {
            let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
            let mut done = recv::Response::<_, RetainOne, _, _>::new();
            done.try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("done reply");
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
            drop(done);
            assert_eq!(rows.count(), 0);

            let mut failed = recv::Response::<_, Never, _, _>::new();
            failed
                .try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("failed reply");
            assert!(
                lane.select(&mut token)
                    .try_fail(Counted::from(&Cause::new(7, &rows)))
                    .is_ok()
            );
            assert_eq!(tags.count(), 2);
            let mut pending = recv::Response::<_, Never, _, _>::new();
            pending
                .try_tagged(&mut token, lane, Counted::data(&tags))
                .expect("pending reply");
            drop(row);
            assert_eq!(rows.count(), 1);
        })
        .expect("reply arena");
    });
    assert_eq!(tags.count(), 3);
    assert_eq!(rows.count(), 2);
}
