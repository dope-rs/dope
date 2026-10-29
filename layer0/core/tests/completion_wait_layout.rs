use std::convert::Infallible;

use dope_core::driver::life::recovery;
use dope_test::scope::Error;
use o3::depot::queue::resident;

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{mem, num};

use dope_core::driver::schedule::ready::{
    wake,
    wake::{fanout::Set, handoff, queue, table},
};
use dope_test::alloc::Tracker;

use crate::common::rt::Runtime;

#[test]
fn registries_use_backing() {
    const CAPACITY: usize = 257;
    const LANE_COUNT: usize = 17;
    const LANES: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(LANE_COUNT - 1);
    const BROADCAST_ENTRY_BYTES: usize = 24;
    const QUEUE_ENTRY_BYTES: usize = 16;
    const TABLE_ENTRY_BYTES: usize = 16;
    const HANDOFF_ENTRY_BYTES: usize = 16;
    const LANE_BYTES: usize = 16;

    let word = mem::size_of::<usize>();
    assert_eq!(word, 8, "supported targets are 64-bit");
    let mut reports = Vec::new();

    Runtime::throughput().with_driver(
        |driver| {
            let driver = driver.driver_ref();
            let (fanout, alloc) = Tracker::usage(|| Set::try_new(driver, CAPACITY));
            let fanout = { (fanout).expect("fanout registry must allocate") };
            assert_eq!(alloc, (1, CAPACITY * BROADCAST_ENTRY_BYTES));

            let (queued, alloc) = Tracker::usage(|| queue::Queue::try_new(driver, CAPACITY, LANES));
            let queued = { (queued).expect("queue registry must allocate") };
            assert_eq!(
                alloc,
                (1, CAPACITY * QUEUE_ENTRY_BYTES + LANE_COUNT * LANE_BYTES,),
            );
            let (exact, alloc) = Tracker::usage(|| table::Table::try_new(driver, CAPACITY));
            let exact = { (exact).expect("exact table registry must allocate") };
            assert_eq!(alloc, (1, CAPACITY * TABLE_ENTRY_BYTES));
            let (handoff, alloc) = Tracker::usage(|| {
                handoff::Queue::<u64, _>::try_new(driver, CAPACITY, resident::Fixed::<LANE_COUNT>)
            });
            let handoff = { (handoff).expect("handoff registry must allocate") };
            assert_eq!(
                alloc,
                (1, CAPACITY * HANDOFF_ENTRY_BYTES + LANE_COUNT * LANE_BYTES)
            );
            assert!(!fanout.has_pending());
            assert!(!queued.has_waiters());
            assert!(matches!(
                queued.try_wake_one(LANE_COUNT - 1),
                Ok(wake::Once::Empty)
            ));
            assert!(matches!(
                queued.try_wake_one(LANE_COUNT),
                Err(bounds)
                    if bounds.index == LANE_COUNT
                        && bounds.len.get() as usize == LANE_COUNT
            ));
            assert!(!exact.has_waiters());
            assert!(handoff.is_empty::<{ LANE_COUNT - 1 }>());

            let (fixed, alloc) = Tracker::usage(|| {
                handoff::Queue::<u64, _>::try_new(driver, CAPACITY, resident::Fixed::<2>)
            });
            let fixed = (fixed).expect("fixed handoff registry must allocate");
            assert_eq!(alloc, (1, CAPACITY * HANDOFF_ENTRY_BYTES + 2 * LANE_BYTES));
            assert_eq!(mem::size_of_val(&fixed), mem::size_of_val(&handoff));
            assert!(fixed.is_empty::<0>());
            assert!(fixed.is_empty::<1>());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        finish,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
