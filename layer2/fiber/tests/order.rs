use std::array;

use dope_fiber::{
    reply,
    reply::{LaneIndex, recv, recv::attach::Attach as _},
};
use dope_test::alloc::Tracker;
use o3::base::cell::region::Token;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

pub mod common;
use crate::common::storage::replies::support::{Pending, layout};

#[test]
fn rotates_without_alloc() {
    let layout = layout(3, 6).expect("each lane and its shared surplus must fit");
    Token::scope(|mut token| {
        let result = reply::Arena::<(), (), u8>::try_with(layout, token.proof(), |arena| {
            let lanes = [
                LaneIndex::ZERO,
                LaneIndex::new::<1>(),
                LaneIndex::new::<2>(),
            ]
            .map(|index| arena.try_lane(index).expect("reply lane"));
            let mut replies: [_; 3] =
                array::from_fn(|_| recv::Response::<(), Pending, (), u8>::new());
            for ((tag, lane), response) in [0, 1, 2].into_iter().zip(lanes).zip(&mut replies) {
                assert!(response.try_tagged(&mut token, lane, tag).is_ok());
            }
            let measure = Tracker::begin();
            for lane in lanes {
                lane.control().activate(&mut token);
            }
            for expected in [0, 1, 2] {
                let selected = arena
                    .pick_active(&mut token)
                    .and_then(|lane| lane.control().front_tag(&token).copied());
                assert_eq!(selected, Some(expected));
            }
            lanes[1].control().deactivate(&mut token);
            lanes[1].control().activate(&mut token);
            lanes[1].control().activate(&mut token);
            for expected in [0, 2, 1, 0] {
                let selected = arena
                    .pick_active(&mut token)
                    .and_then(|lane| lane.control().front_tag(&token).copied());
                assert_eq!(selected, Some(expected));
            }
            for lane in lanes {
                lane.control().deactivate(&mut token);
            }
            assert!(arena.pick_active(&mut token).is_none());
            assert_eq!(measure.finish().allocations, 0);
        });
        assert!(result.is_ok(), "the fixed reply arena must allocate");
    });
}

#[test]
fn preserves_peer_reserve() {
    let layout = (layout(2, 3)).expect("two reserves and one shared entry must fit");
    Token::scope(|mut token| {
        let result = reply::Arena::<(), (), u8>::try_with(layout, token.proof(), |arena| {
            let (left, right) = match (
                arena.try_lane(LaneIndex::ZERO),
                arena.try_lane(LaneIndex::new::<1>()),
            ) {
                (Ok(left), Ok(right)) => Some((left, right)),
                _ => None,
            }
            .expect("both lanes belong to the arena");
            left.control().activate(&mut token);
            right.control().activate(&mut token);
            let mut replies: [_; 3] =
                array::from_fn(|_| recv::Response::<(), Pending, (), u8>::new());
            let measure = Tracker::begin();
            assert!((replies[0].try_tagged(&mut token, left, 10)) == (Ok(())));
            assert!((replies[1].try_tagged(&mut token, left, 11)) == (Ok(())));
            let selected =
                (arena.pick_active(&mut token)).expect("the peer retains its protected entry");
            assert!((replies[2].try_tagged(&mut token, selected, 20)) == (Ok(())));
            assert_eq!(right.control().front_tag(&token).copied(), Some(20));
            assert_eq!(left.control().front_tag(&token).copied(), Some(10));
            assert!(arena.pick_active(&mut token).is_none());
            left.control().deactivate(&mut token);
            left.control().activate(&mut token);
            assert!(arena.pick_active(&mut token).is_none());
            assert_eq!(left.complete(&mut token).get(), 1);
            assert_eq!(left.control().front_tag(&token).copied(), Some(11));
            assert_eq!(left.complete(&mut token).get(), 0);
            assert_eq!(left.control().front_tag(&token), None);
            assert_eq!(right.complete(&mut token).get(), 0);
            assert_eq!(measure.finish().allocations, 0);
        });
        assert!(result.is_ok(), "the fixed reply arena must allocate");
    });
}
