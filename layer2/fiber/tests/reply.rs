use dope_fiber::{
    reply,
    reply::{
        layout,
        layout::{Data, Entries, Error, Lanes, Layout},
        usage::{Bytes, Values},
    },
};

pub mod common;
use crate::common::storage::replies::support::{capacity, limit};

#[test]
fn preserves_reply_layout() {
    use std::mem::{needs_drop, size_of};

    use reply::recv::outcome;

    assert_eq!(
        size_of::<reply::Lane<'static, 'static, 'static, 'static, ()>>(),
        size_of::<usize>() * 2
    );
    assert_eq!(
        size_of::<reply::Control<'static, 'static, 'static, 'static, ()>>(),
        size_of::<reply::Lane<'static, 'static, 'static, 'static, ()>>()
    );
    assert_eq!(
        size_of::<reply::Lane<'static, 'static, 'static, 'static, (), (), u8>>(),
        size_of::<reply::Lane<'static, 'static, 'static, 'static, ()>>()
    );
    assert_eq!(
        size_of::<reply::Control<'static, 'static, 'static, 'static, (), (), u8>>(),
        size_of::<reply::Control<'static, 'static, 'static, 'static, ()>>()
    );
    assert_eq!(
        size_of::<reply::Slot<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<usize>() * 2
    );
    assert_eq!(
        size_of::<outcome::Completion<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<reply::Slot<'static, 'static, 'static, 'static, 'static, ()>>()
    );
    assert_eq!(
        size_of::<outcome::Fault<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<usize>() * 4
    );
    assert_eq!(
        size_of::<outcome::Terminal<'static, 'static, 'static, 'static, 'static, ()>>(),
        size_of::<usize>() * 4
    );
    assert_eq!(
        size_of::<Option<outcome::Terminal<'static, 'static, 'static, 'static, 'static, ()>>>(),
        size_of::<usize>() * 5
    );
    assert!(!needs_drop::<
        reply::Lane<'static, 'static, 'static, 'static, ()>,
    >());
    assert!(!needs_drop::<
        reply::Control<'static, 'static, 'static, 'static, ()>,
    >());
    assert!(!needs_drop::<
        reply::Lane<'static, 'static, 'static, 'static, (), (), u8>,
    >());
    assert!(!needs_drop::<
        reply::Control<'static, 'static, 'static, 'static, (), (), u8>,
    >());
    assert!(!needs_drop::<
        reply::Slot<'static, 'static, 'static, 'static, 'static, ()>,
    >());
    assert!(!needs_drop::<
        outcome::Completion<'static, 'static, 'static, 'static, 'static, ()>,
    >());
    assert!(needs_drop::<
        outcome::Fault<'static, 'static, 'static, 'static, 'static, ()>,
    >());
    assert!(needs_drop::<
        outcome::Terminal<'static, 'static, 'static, 'static, 'static, ()>,
    >());
}

#[test]
fn rejects_invalid_limits() {
    let caps = capacity(2, 2, 2, 4, 4);
    assert_eq!(
        Layout::bounded(caps).expect("bounded layout").limit(),
        limit(2, 3, 3)
    );
    assert!(
        Layout::new(caps, limit(1, 4, 1))
            == Err(Error::ByteLimitUnattainable {
                requested: Bytes::new(4),
                available: Bytes::new(3),
            })
    );
    assert!(
        Layout::new(caps, limit(1, 3, 4))
            == Err(Error::ValueLimitUnattainable {
                requested: Values::new(4),
                available: Values::new(3),
            })
    );
    assert!(
        Layout::new(capacity(2, 2, 1, 4, 4), limit(2, 3, 3),)
            == Err(Error::DataLimitUnattainable {
                requested: Data::new(2),
                available: Data::new(1),
            })
    );

    assert!(
        Layout::new(capacity(3, 2, 1, 1, 1), limit(1, 1, 1),)
            == Err(Error::EntriesBelowLanes {
                entries: Entries::new(2),
                lanes: Lanes::new(3),
            })
    );

    let entries = layout::MAXIMUM_ENTRIES.get() + 1;
    assert!(
        Layout::new(capacity(1, entries, 1, 1, 1), limit(1, 1, 1),)
            == Err(Error::EntriesTooLarge {
                actual: Entries::new(entries),
                maximum: layout::MAXIMUM_ENTRIES,
            })
    );

    if let Ok(data) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert!(
            Layout::new(capacity(2, 2, data, 4, 4), limit(1, 3, 3),)
                == Err(Error::DataTooLarge {
                    actual: Data::new(data),
                    maximum: layout::MAXIMUM_DATA,
                })
        );
    }
}

#[test]
fn rejects_live_pressure() {
    use dope_fiber::reply::{Arena, LaneIndex, recv, recv::attach::Attach as _, usage};
    use o3::base::cell::region;

    use crate::common::storage::replies::support::{Cause, Collected, Counted, Drops, Never};

    let drops = Drops::new();
    let layout = Layout::bounded(capacity(2, 2, 4, 4, 4)).expect("bounded layout");
    region::Token::scope(|mut token| {
        Arena::try_with(layout, token.proof(), |arena| {
            let first = arena.try_lane(LaneIndex::ZERO).expect("first lane");
            let second = arena.try_lane(LaneIndex::new::<1>()).expect("second lane");
            let mut collect = recv::Response::<_, Collected>::new();
            let mut stream = recv::Response::<_, Never>::new();
            collect
                .try_attach(&mut token, first)
                .expect("collect reply");
            stream.try_attach(&mut token, second).expect("stream reply");
            for _ in 0..2 {
                assert!(
                    first
                        .select(&mut token)
                        .try_push(Counted::mixed(&drops))
                        .is_ok()
                );
                assert!(
                    second
                        .select(&mut token)
                        .try_push(Counted::mixed(&drops))
                        .is_ok()
                );
            }
            let row = first
                .select(&mut token)
                .try_push(Counted::mixed(&drops))
                .expect_err("collection must reject live pressure");
            assert!(matches!(
                &row,
                usage::InsertError::Capacity {
                    cause: usage::Capacity::Unavailable(cause),
                    ..
                } if cause.into_parts() == (
                    usage::Resource::Items,
                    usage::Required::new(1),
                    usage::Available::new(0)
                )
            ));
            let mut cause = Counted::from(&Cause::new(7, &drops));
            cause.bytes = 1;
            let cause = first
                .select(&mut token)
                .try_fail(cause)
                .err()
                .expect("collection failure must reject live pressure");
            assert!(matches!(
                &cause,
                usage::InsertError::Capacity {
                    cause: usage::Capacity::Unavailable(cause),
                    ..
                } if cause.into_parts() == (
                    usage::Resource::Bytes,
                    usage::Required::new(1),
                    usage::Available::new(0)
                )
            ));
            assert_eq!(drops.count(), 0, "rejection preserves both owners");
            assert!(matches!(
                second.select(&mut token).try_push(Counted::mixed(&drops)),
                Err(usage::InsertError::Backpressure { .. })
            ));
            drop((row, cause));
        })
        .expect("reply arena");
    });
    assert_eq!(drops.count(), 7);
}
