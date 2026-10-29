use std::cell::RefCell;

use dope_fiber::reply::{LaneIndex, Storage, layout, recv, recv::attach::Attach as _, usage};
use o3::base::cell::region;

pub mod common;
use common::storage::replies::support::{Counted, Drops, Finish, OwnerTag, capacity};

#[test]
fn holds_until_snapshot() {
    let drops = Drops::new();
    let layout = layout::Layout::bounded(capacity(1, 1, 2, 4, 4)).expect("reply layout");
    let reserved = usage::Usage::new(usage::Bytes::new(4), usage::Values::new(4));
    region::Token::scope(|mut token| {
        Storage::try_with(layout, token.proof(), |storage| {
            let owner = RefCell::new(None);
            storage.try_arena(|arena| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                owner.replace(Some(lane.try_reserve(reserved).expect("payload")));
                let mut response = recv::Response::<_, Finish, Counted<'_>, _>::new();
                response
                    .try_tagged(&mut token, lane, OwnerTag(&owner))
                    .expect("reply attachment");
                assert!(response.try_take(&mut token).is_none());
                assert!(matches!(
                    lane.select(&mut token).try_push(Counted::mixed(&drops)),
                    Err(usage::InsertError::Capacity {
                        cause: usage::Capacity::Unavailable(_),
                        ..
                    })
                ));
                let held = lane
                    .control()
                    .front_tag(&token)
                    .expect("ordered reply")
                    .0
                    .take()
                    .expect("reserved owner");
                assert_eq!(held.usage(), reserved);
                drop(held);
                assert!(
                    lane.select(&mut token)
                        .try_push(Counted::mixed(&drops))
                        .is_ok()
                );
                assert!(matches!(
                    lane.try_reserve(reserved),
                    Err(usage::Capacity::Unavailable(_))
                ));
                lane.complete(&mut token);
                assert!(matches!(response.try_take(&mut token), Some(Ok(()))));
                assert!(response.try_take(&mut token).is_none());
            })
        })
        .expect("reply backing")
        .expect("reply arena");
    });
    assert_eq!(drops.count(), 2);
}
