use dope_core::driver::life::creation;

use crate::common::rt::profile::{Batched, Prompt};
pub mod common;

use std::mem::size_of;

use dope_core::{
    driver::{
        result::CreateError,
        route::{
            Epoch, Id, KeyTag, Route, SLOT_MASK, Token,
            indices::{Edge, SlotIndex},
            kind,
            table::{Capacity, Parts, Slab, bounds, conn},
        },
        settings,
        settings::{
            demands,
            demands::{Accept, Files, Outbound, Submit},
            errors::{Claim, Config},
        },
    },
    random::Seed,
};
use settings::demands::slots;

use crate::common::{driver::support::ROUTE, rt::Runtime};

#[test]
fn tokens_preserve_identity() {
    let parts = Parts::<KeyTag<ROUTE>>::new(SlotIndex::ZERO, Epoch::INITIAL);
    let target = Token::from_parts(parts);
    assert!((target.epoch()) == (Epoch::INITIAL));
    assert!((parts.epoch()) == (Epoch::INITIAL));
    assert!((Token::from_parts(parts)) == (target));

    let app = Route::from(Id::fixed::<0>());
    assert!((Id::try_from(app).map(Route::from)) == (Ok(app)));
}

#[test]
fn segments_report_bounds() {
    let one = (conn::Capacity::try_from(1)).expect("one connection was rejected");
    let base = Edge::from(one);
    let capacity = settings::FileSlots::fixed::<0, 2>().outbound();
    let result = bounds::Segment::try_new(base, capacity);
    let segment = result.expect("bounded segment");
    let index = segment
        .try_index(SlotIndex::ZERO)
        .expect("first segment slot");
    assert!(index.local() == SlotIndex::ZERO);
    assert!(index.physical() == SlotIndex::from(1_u16));
    assert!(segment.offset(index.physical()) == Ok(index));
    let outside = SlotIndex::from(2_u16);
    assert_eq!(segment.try_index(outside).err(), capacity.excludes(outside));
    assert_eq!(segment.indices().count(), capacity.count().get());
    let empty = bounds::Segment::try_new(base, Capacity::try_from(0).expect("empty capacity"))
        .expect("empty segment");
    assert!(empty.try_index(SlotIndex::ZERO).is_err());
    assert!(empty.indices().next().is_none());
    assert!(
        (segment.offset(SlotIndex::ZERO))
            == (Err(bounds::Offset::Before {
                actual: SlotIndex::ZERO,
                base
            }))
    );
    assert!(matches!(
        segment.offset(SlotIndex::from(3_u16)),
        Err(bounds::Offset::Capacity(_))
    ));
    let full = (Capacity::try_from(SLOT_MASK as usize + 1)).expect("full capacity was rejected");
    assert!(bounds::Segment::try_new(base, full).is_err());
}

#[test]
fn slots_enforce_raw_bounds() {
    let overflow = SLOT_MASK as u32 + 1;
    assert!(SlotIndex::try_from(SLOT_MASK as u32).is_ok());
    assert_eq!(
        SlotIndex::try_from(overflow)
            .err()
            .map(|error| error.actual()),
        Some(overflow as usize),
    );
    assert_eq!(
        SlotIndex::try_from(overflow as usize)
            .err()
            .map(|error| error.actual()),
        Some(overflow as usize),
    );

    let indexed = (Capacity::try_from(SLOT_MASK as usize)).expect("sentinel capacity was rejected");
    assert_eq!(indexed.count().get(), SLOT_MASK as usize);
    assert!(
        indexed.into_iter().next_back() == Some(SlotIndex::fixed::<{ SLOT_MASK as u32 - 1 }>())
    );

    let full =
        (Capacity::try_from(SLOT_MASK as usize + 1)).expect("full token capacity was rejected");
    assert_eq!(full.count().get(), SLOT_MASK as usize + 1);
    assert!(full.into_iter().next_back() == Some(SlotIndex::MAX));

    assert!(conn::Capacity::try_from(0).is_err());
    let conns = (conn::Capacity::try_from(SLOT_MASK as usize))
        .expect("maximum connection capacity was rejected");
    assert_eq!(conns.entries().get(), SLOT_MASK as usize);
    assert!((Capacity::from(conns)) == (indexed));
    assert!(Edge::from(conns).slot() == SlotIndex::MAX);
    assert!(conn::Capacity::try_from(SLOT_MASK as usize + 1).is_err());
    assert_eq!(size_of::<conn::Capacity>(), size_of::<Capacity>());
}

#[test]
fn skips_preserve_bounds() {
    for count in 0..8 {
        for front in 0..10 {
            for back in 0..10 {
                let capacity = Capacity::try_from(count).expect("small capacity");
                let mut slots = capacity.into_iter();
                let mut expected =
                    (0..count).map(|raw| SlotIndex::try_from(raw).expect("small slot"));
                assert_eq!(slots.nth(front), expected.nth(front));
                assert_eq!(slots.nth_back(back), expected.nth_back(back));
                assert_eq!(slots.next_back(), expected.next_back());
                assert_eq!(slots.size_hint(), expected.size_hint());
                assert!(slots.eq(expected));
            }
        }
    }
    let full = Capacity::try_from(SLOT_MASK as usize + 1).expect("full capacity");
    assert_eq!(
        full.into_iter().nth(SLOT_MASK as usize),
        Some(SlotIndex::MAX)
    );
    assert_eq!(
        full.into_iter().nth_back(SLOT_MASK as usize),
        Some(SlotIndex::ZERO)
    );
    assert_eq!(full.into_iter().nth(usize::MAX), None);
    assert_eq!(full.into_iter().nth_back(usize::MAX), None);
}

#[test]
fn file_layout_resolves() {
    let base = settings::FileSlots::fixed::<0, 1>();
    assert!(
        base.resolve(Files::accept(Accept::from(
            settings::demands::slots::Slots::fixed::<1>()
        ))) == Ok(settings::FileSlots::fixed::<1, 1>())
    );
    let accepted = SLOT_MASK as u32 + 2;
    assert!(
        base.resolve(Files::accept(Accept::from(
            settings::demands::slots::Slots::fixed::<{ SLOT_MASK as u32 + 2 }>()
        ),)) == Err(Config::DescriptorCapacityOutOfRange {
            accepted,
            outbound: 1,
            actual: u64::from(accepted) + 1,
            minimum: 1,
            maximum: SLOT_MASK as u32 + 1,
        })
    );
    assert!(
        base.resolve(Files::new(
            Accept::from(settings::demands::slots::Slots::fixed::<16>()),
            Outbound::fixed::<{ u32::MAX }>()
        )) == Err(Config::DescriptorCapacityOutOfRange {
            accepted: 16,
            outbound: u32::MAX,
            actual: u64::from(u32::MAX) + 16,
            minimum: 1,
            maximum: SLOT_MASK as u32 + 1,
        })
    );
    let split = settings::FileSlots::fixed::<65_279, 256>();
    assert!((split.capacity()) == (settings::FileSlots::fixed::<0, 65_535>().outbound()));
    assert!((split.accept()) == (settings::FileSlots::fixed::<0, 65_279>().outbound()));
    assert!((split.outbound()) == (settings::FileSlots::fixed::<0, 256>().outbound()));
    assert_eq!(size_of::<settings::FileSlots>(), size_of::<[u32; 2]>());
}

#[test]
fn queue_layout_resolves() {
    use settings::QueueLayout;

    let base = QueueLayout::fixed::<8, 8>();
    assert!(base.resolve(Submit::from(0)) == Ok(base));
    assert!(base.resolve(Submit::from(9)) == Ok(QueueLayout::fixed::<16, 16>()));
    assert!(
        base.resolve(Submit::from(32_769))
            == Err(Claim::Layout(Config::QueueSubmissionsOutOfRange {
                actual: 65_536,
                minimum: 8,
                maximum: 32_768,
            }))
    );
    assert!(base.resolve(Submit::from(32_768)) == Ok(QueueLayout::fixed::<32_768, 32_768>()));
    assert!(
        QueueLayout::fixed::<32_768, 65_536>().resolve(Submit::from(32_768))
            == Ok(QueueLayout::fixed::<32_768, 65_536>())
    );
    assert_eq!(size_of::<QueueLayout>(), size_of::<[u32; 2]>());
    assert!((settings::Config::default().queue_layout()) == (QueueLayout::fixed::<1024, 2048>()));
    assert!(
        (Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE)
            .queue_layout())
            == (QueueLayout::fixed::<256, 1024>())
    );
}

#[test]
fn keeps_completion_policy() {
    assert_eq!(size_of::<settings::Progress>(), size_of::<bool>());
    assert_eq!(size_of::<settings::Config>(), size_of::<[u32; 10]>());
    assert!((settings::Config::default().progress()) == (settings::Progress::Prompt));
    assert!(
        (Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE)
            .progress())
            == (settings::Progress::Prompt)
    );
    assert!((settings::Config::for_profile::<Prompt>().progress()) == (settings::Progress::Prompt));
    assert!(
        (settings::Config::for_profile::<Batched>().progress())
            == (settings::Progress::BatchedWhenSupported)
    );
}

#[test]
fn keeps_schedule_capacity() {
    use settings::Schedule;
    use slots::Slots;

    assert_eq!(Slots::ZERO.count().get(), 0);
    let base = Schedule::fixed::<0, 0>();
    let maximum = base.resolve(demands::Schedule::new(Slots::MAX, Slots::ZERO));
    assert_eq!(maximum.ready().count().get(), u32::MAX as usize);
    if let Some(overflow) = (u32::MAX as usize).checked_add(1) {
        assert!(Slots::try_from(overflow).is_err());
    }
    assert_eq!(size_of::<Slots>(), size_of::<u32>());

    let layout = Schedule::fixed::<7, 11>();
    assert_eq!(layout.ready().count().get(), 7);
    assert_eq!(layout.timers().count().get(), 11);
    assert_eq!(size_of::<Schedule>(), size_of::<[u32; 2]>());

    let changed = settings::Schedule::fixed::<0, 0>().resolve(demands::Schedule::new(
        Slots::fixed::<13>(),
        layout.timers(),
    ));
    assert_eq!(changed.ready().count().get(), 13);
    assert!((changed.timers()) == (layout.timers()));

    let changed = settings::Schedule::fixed::<0, 0>()
        .resolve(demands::Schedule::new(layout.ready(), Slots::ZERO));
    assert!((changed.ready()) == (layout.ready()));
    assert!((changed.timers()) == (Slots::ZERO));
    assert!(
        (Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE)
            .schedule())
            == (Schedule::DEFAULT)
    );
}

#[test]
fn rejects_ready_overflow() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let layout = settings::Schedule::fixed::<0, 0>().resolve(demands::Schedule::new(
        slots::Slots::MAX,
        config.schedule().timers(),
    ));
    let expected = Config::ReadyCapacity {
        files: 16,
        ready: u32::MAX,
        maximum: u32::from(u16::MAX),
    };
    let mut creation = creation::Attempt::new();
    assert!(matches!(
        creation.try_driver(config.set_schedule(layout)),
        Err(CreateError::Plan(actual)) if *actual == expected
    ));
    assert!(matches!(
        creation.try_driver(config),
        Err(CreateError::Plan(_))
    ));
    let seed = Seed::acquire().expect("driver seed");
    assert!(matches!(
        creation.try_seed(config, seed),
        Err(CreateError::Plan(_))
    ));
    let seed = Seed::acquire().expect("driver seed");
    assert!(matches!(
        creation::Attempt::new().try_seed(config.set_schedule(layout), seed),
        Err(CreateError::Plan(actual)) if *actual == expected
    ));
}

#[test]
fn slabs_bound_keys() {
    let capacity = settings::FileSlots::fixed::<0, 1>().outbound();
    let mut slab =
        (Slab::<_, KeyTag<ROUTE, kind::Send>>::try_new(capacity)).expect("slab allocation failed");
    let vacant = (slab.vacancy().try_reserve()).expect("empty slab rejected its slot");
    let key = vacant.key();
    vacant.insert(7);
    let token = Token::from_parts(key.parts());
    assert!(token.slot() == SlotIndex::ZERO);
    assert!((token.kind()) == (Ok(kind::Kind::Send)));
    assert_eq!(slab.view().get(key.parts()), Some(&7));

    let overflow = SLOT_MASK as usize + 2;
    assert!(Capacity::try_from(overflow).is_err());
}

#[test]
fn slabs_reject_stale_keys() {
    let capacity = settings::FileSlots::fixed::<0, 1>().outbound();
    let mut slab = (Slab::<_, KeyTag<ROUTE>>::try_new(capacity)).expect("slab allocation failed");
    let canceled = {
        let vacant = (slab.vacancy().try_reserve()).expect("initial route reservation failed");
        vacant.key()
    };
    let vacant = (slab.vacancy().try_reserve()).expect("empty slab rejected its slot");
    let stale = vacant.key();
    vacant.insert(7);
    assert!(canceled.slot() == stale.slot());
    assert!(canceled.epoch() != stale.epoch());
    assert_eq!(slab.view().get(canceled.parts()), None);
    assert_eq!(slab.view().get(stale.parts()), Some(&7));
    assert_eq!(
        slab.access()
            .occupied(stale.parts())
            .map(|entry| entry.remove()),
        Some(7)
    );
    let vacant = (slab.vacancy().try_reserve()).expect("removed slot was not reusable");
    let current = vacant.key();
    vacant.insert(11);
    assert!(current.slot() == stale.slot());
    assert!((current.epoch()) != (stale.epoch()));
    assert_eq!(slab.view().get(stale.parts()), None);
    assert_eq!(slab.view().get(current.parts()), Some(&11));
}
