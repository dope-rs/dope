pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{cell, mem, ptr};

use dope_core::driver::schedule::clock::timer::heap;
use dope_test::alloc::Tracker;
use o3::alloc;

use crate::common::timer::{Key, Value};

#[test]
fn aligned_owner() {
    for (capacity, bytes) in [(0, 0), (1, 512), (3, 1024)] {
        let drops = cell::Cell::new(0);
        let ((), lifetime) = Tracker::measure(|| {
            let (made, cold) =
                Tracker::measure(|| heap::raw::Store::<Key, Value<'_>>::try_new(capacity));
            assert_eq!(cold, [usize::from(capacity != 0), bytes, 0, 0]);
            let store = (made).expect("the aligned four-plane store must allocate");
            assert_eq!(mem::size_of_val(&store), 6 * mem::size_of::<usize>());
            let ((), hot) = Tracker::measure(|| {
                // SAFETY: this test owns `store` exclusively, so no other loan overlaps.
                unsafe {
                    store.loan(|mut heap| {
                        for index in 0..capacity {
                            let vacant =
                                (heap.vacancy()).expect("every admitted slot must be vacant");
                            let lease = vacant.lease(Value(&drops));
                            heap.set(&lease, Key(index as u8));
                            assert_eq!(heap.priority(&lease).map(|key| key.0), Some(index as u8));
                            assert_eq!(heap.resident(&lease).0.get(), 0);
                            assert_eq!(ptr::from_ref(heap.resident(&lease)).addr() % 128, 0);
                        }
                    })
                }
            });
            assert_eq!(hot, [0; 4]);
            assert_eq!(drops.get(), 0);
        });
        let count = usize::from(capacity != 0);
        assert_eq!(lifetime, [count, bytes, count, bytes]);
        assert_eq!(drops.get(), capacity);
    }
}

#[test]
fn failure_boundary() {
    let (result, counts) = Tracker::measure(|| {
        Tracker::fail_after(0, || heap::raw::Store::<Key, Value<'_>>::try_new(1))
    });
    assert!(matches!(result, Err(alloc::Error::Exhausted)));
    assert_eq!(counts, [0; 4]);
    let (result, counts) =
        Tracker::measure(|| Tracker::fail_after(0, || heap::raw::Store::<(), ()>::try_new(0)));
    assert!(result.is_ok());
    assert_eq!(counts, [0; 4]);
    let (result, counts) =
        Tracker::measure(|| heap::raw::Store::<(), ()>::try_new(u32::MAX as usize));
    assert!(matches!(result, Err(alloc::Error::Overflow)));
    assert_eq!(counts, [0; 4]);
}

#[test]
fn last_plane_overflow() {
    let (result, counts) = Tracker::measure(|| {
        Tracker::fail_after(0, || {
            heap::raw::Store::<(), [u8; 1 << (usize::BITS / 2)]>::try_new(
                1 << (usize::BITS / 2 - 1),
            )
        })
    });
    assert!(matches!(result, Err(alloc::Error::Overflow)));
    assert_eq!(counts, [0; 4]);
}

#[test]
fn loan_keeps_owner() {
    const { assert!(!mem::needs_drop::<heap::Min<'_, '_, (), Value<'_>>>()) };
    let drops = cell::Cell::new(0);
    {
        let store = heap::raw::Store::<(), Value<'_>>::try_new(1).expect("one slot");
        // SAFETY: this test owns `store` exclusively, so no other loan overlaps.
        unsafe {
            store.loan(|mut heap| {
                let lease = heap.vacancy().expect("vacant slot").lease(Value(&drops));
                assert_eq!(heap.resident(&lease).0.get(), 0);
            });
        }
        assert_eq!(drops.get(), 0);
        // SAFETY: the previous exclusive loan ended before this call.
        unsafe { store.loan(|mut heap| assert!(heap.vacancy().is_none())) };
    }
    assert_eq!(drops.get(), 1);
}
