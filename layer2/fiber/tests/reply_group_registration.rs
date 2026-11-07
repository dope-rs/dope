use std::{cell, convert};

use dope_fiber::reply::{self, Arena, LaneIndex, layout, order, recv, recv::attach::Attach as _};
use o3::base::cell::region;

pub mod common;
use common::storage::replies::support::{Counted, Drops, Pending, capacity};

#[test]
fn constructs_after_admission() {
    let drops = Drops::new();
    let calls = cell::Cell::new(0);
    let layout = layout::Layout::bounded(capacity(1, 1, 1, 1, 1)).expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::<()>::try_with(layout, token.proof(), |arena| {
            let lane = arena.try_lane(LaneIndex::ZERO).expect("lane");
            let make = |input| {
                calls.set(calls.get() + 1);
                (input, ())
            };
            let (depth, response, ()) = recv::Response::<_, Pending<Counted<'_>>>::try_with(
                &mut token,
                lane,
                (),
                Counted::data(&drops),
                make,
            )
            .ok()
            .expect("admitted response");
            assert_eq!(depth.get(), 1);
            let (cause, input) = recv::Response::<_, Pending<Counted<'_>>>::try_with(
                &mut token,
                lane,
                (),
                Counted::data(&drops),
                make,
            )
            .err()
            .expect("full lane");
            assert!(matches!(cause, recv::attach::Error::Full { .. }));
            assert_eq!(calls.get(), 1);
            assert_eq!(drops.count(), 0);
            drop((response, input));
            assert_eq!(drops.count(), 2);
        })
        .expect("reply arena");
    });
}

#[test]
fn admission_is_atomic() {
    const COMMANDS: usize = 257;
    const CAPACITY: usize = COMMANDS + 4;
    let layout = layout::Layout::bounded(capacity(3, CAPACITY, CAPACITY, CAPACITY, CAPACITY))
        .expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::<(), (), usize>::try_with(layout, token.proof(), |arena| {
            let first = arena.try_lane(LaneIndex::ZERO).expect("first lane");
            let second = arena.try_lane(LaneIndex::new::<1>()).expect("second lane");
            let third = arena.try_lane(LaneIndex::new::<2>()).expect("third lane");
            first.control().activate(&mut token);
            third.control().activate(&mut token);

            let mut full = [const { recv::Response::<_, Pending, (), usize>::new() }; COMMANDS + 2];
            let mut terminal = recv::Response::<_, Pending, (), usize>::new();
            assert!(matches!(
                recv::Response::try_group_by(
                    &mut token,
                    first,
                    &mut full,
                    &mut terminal,
                    ([0; COMMANDS + 2], 0, ()),
                    convert::identity,
                ),
                Err((recv::attach::Error::Oversized { requested, capacity }, _))
                    if requested.get() == COMMANDS + 3 && capacity.get() == COMMANDS + 2
            ));

            let mut held = [const { recv::Response::<_, Pending, (), usize>::new() }; 2];
            for reply in &mut held {
                reply
                    .try_tagged(&mut token, second, 20)
                    .expect("held reply");
            }

            let mut denied =
                [const { recv::Response::<_, Pending, (), usize>::new() }; COMMANDS + 1];
            assert!(matches!(
                recv::Response::try_group_by(
                    &mut token,
                    first,
                    &mut denied,
                    &mut terminal,
                    ([0; COMMANDS + 1], 0, ()),
                    convert::identity,
                ),
                Err((recv::attach::Error::Capacity { .. }, _))
            ));
            assert_eq!(arena.inflight().get(), 2);
            assert_eq!(first.select(&mut token).front(), order::Front::Empty);
            assert!(matches!(
                first.control().attachment(&mut token),
                reply::Attachment::Detached
            ));

            let mut commands = [const { recv::Response::<_, Pending, (), usize>::new() }; COMMANDS];
            recv::Response::try_group_by(
                &mut token,
                first,
                &mut commands,
                &mut terminal,
                (std::array::from_fn(|index| index + 11), COMMANDS + 11, ()),
                convert::identity,
            )
            .expect("group attachment");
            assert_eq!(arena.inflight().get(), COMMANDS + 3);
            for _ in 0..2 {
                let selected = arena.pick_active(&mut token).expect("reserved lane");
                assert!(matches!(
                    selected.control().registration(&mut token),
                    reply::Registration::Available
                ));
            }
            for tag in 11..=COMMANDS + 11 {
                assert_eq!(first.control().front_tag(&token).copied(), Some(tag));
                first.complete(&mut token);
            }
            assert_eq!(first.select(&mut token).front(), order::Front::Empty);

            let mut other = recv::Response::<_, Pending, (), usize>::new();
            other
                .try_tagged(&mut token, third, 21)
                .expect("reserved lane");
            assert_eq!(arena.inflight().get(), CAPACITY);
            assert!(arena.pick_active(&mut token).is_none());
        })
        .expect("reply arena");
    });
}

#[test]
fn rejection_returns_owners() {
    let drops = Drops::new();
    let calls = cell::Cell::new(0);
    let layout = layout::Layout::bounded(capacity(1, 3, 3, 3, 3)).expect("reply layout");
    region::Token::scope(|mut token| {
        Arena::<(), (), Counted<'_>>::try_with(layout, token.proof(), |arena| {
            let lane = arena.try_lane(LaneIndex::ZERO).expect("lane");
            let mut held = recv::Response::<_, Pending, (), _>::new();
            held.try_tagged(&mut token, lane, Counted::data(&drops))
                .expect("held reply");
            let mut commands = [const { recv::Response::<_, Pending, (), _>::new() }; 2];
            let mut terminal = recv::Response::<_, Pending, (), _>::new();
            let (cause, ([first, unused], tag, owner)) = recv::Response::try_group_by(
                &mut token,
                lane,
                &mut commands,
                &mut terminal,
                (
                    [Counted::data(&drops), Counted::data(&drops)],
                    Counted::data(&drops),
                    Counted::data(&drops),
                ),
                |input| {
                    calls.set(calls.get() + 1);
                    input
                },
            )
            .err()
            .expect("full group");
            assert!(matches!(cause, recv::attach::Error::Full { .. }));
            assert_eq!(arena.inflight().get(), 1);
            assert_eq!(drops.count(), 0);
            assert_eq!(calls.get(), 0);
            drop(unused);
            let mut command = [recv::Response::<_, Pending, (), _>::new()];
            let (depth, owner) = recv::Response::try_group_by(
                &mut token,
                lane,
                &mut command,
                &mut terminal,
                ([first], tag, owner),
                |input| {
                    calls.set(calls.get() + 1);
                    input
                },
            )
            .ok()
            .expect("admitted group");
            assert_eq!(depth.get(), 3);
            assert_eq!(arena.inflight().get(), 3);
            assert_eq!(drops.count(), 1);
            assert_eq!(calls.get(), 1);
            drop(owner);
            assert_eq!(drops.count(), 2);
        })
        .expect("reply arena");
    });
    assert_eq!(drops.count(), 5);
}
