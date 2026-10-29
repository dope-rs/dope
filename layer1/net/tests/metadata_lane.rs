use std::{cell::Cell, mem, num::NonZero};

use common::metadata::Counted;
use dope_net::link::egress::{
    self, Config,
    data::compose,
    metadata::{self, Lane, LaneCount, arena},
};
use dope_test::alloc::Tracker;
pub mod common;
use o3::base::cell::region;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reports_lane_count() {
    let actual = NonZero::<usize>::MIN.saturating_add(u32::MAX as usize - 1);
    let observed = LaneCount::try_new(actual);

    assert!(matches!(observed, Err(cause) if cause.actual() == actual));
    assert_eq!(metadata::Count::MAXIMUM, actual.get() - 1);
}

#[test]
fn arena_keeps_lane_count() {
    let actual = NonZero::<usize>::MIN.saturating_add(u32::MAX as usize - 1);

    let observed = arena::Store::try_new(Config::DEFAULT, actual);

    assert!(matches!(
        observed,
        Err(arena::CreateError::Lanes(cause)) if cause.actual() == actual
    ));
}

#[test]
fn reuses_credit_owner() {
    region::Token::scope(|mut token| {
        let store = arena::Store::try_new(
            Config::shared(egress::Credit::fixed::<1, 0>()),
            NonZero::<usize>::MIN,
        );
        let mut store = (store).expect("required fixture outcome");

        for _ in 0..2 {
            let arena = arena::Arena::<&[u8]>::try_new(&token, &mut store);
            let arena = (arena).expect("required fixture outcome");
            let slot = arena.get(Lane::at(NonZero::<u32>::MIN));
            assert!(slot.is_some());
            let Some(slot) = slot else { return };
            let queue = slot.queue();

            let ((), alloc) = Tracker::usage(|| {
                let value: &[u8] = b"held";
                assert!(matches!(
                    queue.admit(&mut token, egress::Load::new(value)),
                    metadata::Admission::Committed
                ));
                {
                    let front = queue.take_front(&mut token);
                    assert!(front.is_some());
                    let Some(front) = front else { return };
                    assert_eq!(front.get().as_ptr(), value.as_ptr());
                    front.restore();
                }
                let front = queue.take_front(&mut token);
                assert!(front.is_some());
                let Some(front) = front else { return };
                assert_eq!(front.into_inner().as_ptr(), value.as_ptr());
                assert!(queue.is_empty());
            });
            assert_eq!(alloc, (0, 0));
        }
    });
}

#[test]
fn drops_before_reuse() {
    let drops = Cell::new(Some(0));
    region::Token::scope(|mut token| {
        let mut store = arena::Store::try_lanes(
            Config::shared(egress::Credit::fixed::<2, 0>()),
            LaneCount::fixed::<1>(),
        )
        .expect("metadata store");
        for pass in 1..=2 {
            {
                let arena = arena::Arena::<compose::Tagged<&[u8], Counted<'_>>>::try_new(
                    &token, &mut store,
                )
                .expect("metadata arena");
                let queue = arena
                    .get(Lane::at(NonZero::<u32>::MIN))
                    .expect("first lane")
                    .queue();
                for bytes in [&b"first"[..], &b"second"[..]] {
                    assert!(matches!(
                        queue.admit(
                            &mut token,
                            egress::Load::new(compose::Tagged::new(bytes, Counted(&drops))),
                        ),
                        metadata::Admission::Committed,
                    ));
                }
            }
            assert_eq!(drops.get(), Some(pass * 2));
        }
    });
}

#[test]
fn reuses_forgotten_store() {
    region::Token::scope(|mut token| {
        let mut store = arena::Store::try_lanes(
            Config::shared(egress::Credit::fixed::<1, 0>()),
            LaneCount::fixed::<1>(),
        )
        .expect("metadata store");
        for _ in 0..2 {
            let arena = arena::Arena::<&[u8]>::try_new(&token, &mut store).expect("metadata arena");
            let queue = arena
                .get(Lane::at(NonZero::<u32>::MIN))
                .expect("first lane")
                .queue();
            assert!(matches!(
                queue.admit(&mut token, egress::Load::new(&b"held"[..])),
                metadata::Admission::Committed,
            ));
            mem::forget(queue.take_front(&mut token).expect("committed front"));
        }
    });
}
