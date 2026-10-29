pub mod common;
use crate::common::contract::credits::Context;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{cell::Cell, hint, mem, num::NonZero};

use dope_core::driver::{
    receive::credit::{error, storage},
    route::indices,
};
use dope_test::alloc::Tracker;

fn record<C>(context: &(C, &Cell<Option<NonZero<u64>>>), target: NonZero<u64>) {
    assert_eq!(
        context.1.replace(Some(target)),
        None,
        "an unexpected callback must be observed once"
    );
}

#[test]
fn allocation_reuses_group() {
    let capacity = NonZero::<u32>::MIN.saturating_add(16);
    let smaller = NonZero::<u32>::MIN.saturating_add(10);
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(41);
    let old = {
        let (result, alloc) = Tracker::measure(|| owner.try_slots(capacity));
        let slots = result.expect("required fixture outcome");
        assert_eq!(alloc, [1, (6 + 32 * 2) * mem::size_of::<usize>(), 0, 0]);
        assert_eq!(mem::size_of_val(&slots), 4 * mem::size_of::<usize>());
        let slot = slots
            .get(indices::SlotIndex::fixed::<16>())
            .expect("active index");
        let old = slot.key();
        assert_eq!(*old.context(), 41);
        old
    };
    assert!(matches!(
        old.arm(NonZero::<u64>::MIN),
        Err(error::Arm::Stale(_))
    ));
    let (result, alloc) = Tracker::measure(|| owner.try_slots(smaller));
    let slots = result.expect("required fixture outcome");
    assert_eq!(alloc, [0, 0, 0, 0]);
    assert!(matches!(
        old.arm(NonZero::<u64>::MIN),
        Err(error::Arm::Stale(_))
    ));
    assert!(
        matches!(slots.get(indices::SlotIndex::fixed::<16>()), Err(error::Index { requested, capacity }) if requested == indices::SlotIndex::fixed::<16>() && capacity == smaller)
    );
    let slot = slots
        .get(indices::SlotIndex::fixed::<10>())
        .expect("smaller active index");
    assert!(!slot.is_active());
    assert_eq!(*old.context(), 41);
}

#[test]
fn bounds_growth() {
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(());
    let ((), alloc) = Tracker::measure(|| {
        for count in 1..=65_u16 {
            let capacity = NonZero::<u32>::MIN.saturating_add(u32::from(count) - 1);
            let slots = owner.try_slots(capacity).expect("credit storage");
            assert_eq!(slots.iter().count(), usize::from(count));
            assert!(matches!(
                slots.get(indices::SlotIndex::from(count)),
                Err(error::Index { capacity: actual, .. }) if actual == capacity
            ));
        }
    });
    assert!(alloc[0] <= 8);
    assert!(alloc[1] <= (8 * 6 + 4 * 65 * 2) * mem::size_of::<usize>());
}

#[test]
fn lease_delays_reuse() {
    let observed = Cell::new(None);
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(((), &observed));
    let (old, lease) = {
        let slots = owner
            .try_slots(NonZero::<u32>::MIN)
            .expect("initial allocation");
        let slot = slots
            .get(indices::SlotIndex::fixed::<0>())
            .expect("active slot");
        let old = slot.key();
        let lease = old.arm(NonZero::<u64>::MIN).expect("idle slot");
        (old, lease)
    };
    let (result, alloc) = Tracker::measure(|| owner.try_slots(NonZero::<u32>::MIN));
    let busy = result.expect("held group requires allocation");
    assert_eq!(alloc, [1, 8 * mem::size_of::<usize>(), 0, 0]);
    lease.release_with(NonZero::<u8>::MIN, record);
    let (result, alloc) = Tracker::measure(|| owner.try_slots(NonZero::<u32>::MIN));
    let slots = result.expect("released group is reusable");
    assert_eq!(alloc, [0, 0, 0, 0]);
    assert!(matches!(
        old.arm(NonZero::<u64>::MIN),
        Err(error::Arm::Stale(_))
    ));
    drop((slots, busy));
    assert_eq!(observed.get(), None);
}

#[test]
fn edges_are_exact() {
    let target = NonZero::<u64>::MIN.saturating_add(4);
    let signal = NonZero::<u8>::MIN.saturating_add(8);
    let sent = Cell::new(None);
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(41);
    let slots = owner
        .try_slots(NonZero::<u32>::MIN)
        .expect("initial allocation");
    let slot = slots
        .get(indices::SlotIndex::fixed::<0>())
        .expect("active slot");
    let key = slot.key();
    let (armed, alloc) = Tracker::measure(|| key.arm(target));
    let lease = armed.expect("idle slot");
    assert_eq!(alloc, [0, 0, 0, 0]);
    lease.release_with(signal, |context, target| {
        assert_eq!(*context, 41);
        sent.set(Some(target));
    });
    assert_eq!(sent.get(), Some(target));
    assert!(matches!(key.arm(target), Err(error::Arm::Busy)));
    assert_eq!(slot.take(), Some(signal));
    assert_eq!(slot.take(), None);
    let lease = key.arm(target).expect("rearm checked slot");
    assert!(matches!(key.arm(target), Err(error::Arm::Busy)));
    drop(lease);
    assert!(!slot.is_active());
    assert_eq!(slot.take(), None);
}

#[test]
fn detach_revokes_ready() {
    let observed = Cell::new(None);
    let target = NonZero::<u64>::MIN;
    let signal = NonZero::<u8>::MIN;
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(((), &observed));
    let key = {
        let slots = owner
            .try_slots(NonZero::<u32>::MIN.saturating_add(2))
            .expect("initial allocation");
        let ready = slots
            .get(indices::SlotIndex::fixed::<0>())
            .expect("first slot");
        let lease = ready.key().arm(target).expect("idle slot");
        lease.release_with(signal, |context, value| {
            assert_eq!(context.0, ());
            assert_eq!(value, target);
        });
        ready.detach();
        assert_eq!(ready.take(), None);
        assert!(matches!(ready.key().arm(target), Err(error::Arm::Detached)));
        let held = slots
            .get(indices::SlotIndex::fixed::<1>())
            .expect("second slot");
        let key = held.key();
        let lease = key.arm(target).expect("second slot is idle");
        held.detach();
        assert!(held.is_active());
        assert!(matches!(key.arm(target), Err(error::Arm::Detached)));
        lease.release_with(signal, record);
        assert!(!held.is_active());
        let cancel = slots
            .get(indices::SlotIndex::fixed::<2>())
            .expect("third slot");
        let lease = cancel.key().arm(target).expect("third slot idle");
        cancel.detach();
        drop(lease);
        assert!(!cancel.is_active());
        assert!(matches!(
            cancel.key().arm(target),
            Err(error::Arm::Detached)
        ));
        key
    };
    assert!(matches!(key.arm(target), Err(error::Arm::Stale(_))));
    assert_eq!(observed.get(), None);
}

#[test]
fn context_follows_owner() {
    let drops = Cell::new(0);
    let release = {
        let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(Context(&drops));
        {
            let slots = owner
                .try_slots(NonZero::<u32>::MIN)
                .expect("first allocation");
            let slot = slots
                .get(indices::SlotIndex::fixed::<0>())
                .expect("active slot");
            assert!(std::ptr::eq(slot.key().context(), owner.context()));
        }
        let moved = hint::black_box(owner);
        {
            let (result, alloc) = Tracker::measure(|| moved.try_slots(NonZero::<u32>::MIN));
            assert_eq!(alloc, [0, 0, 0, 0]);
            let slots = result.expect("reuse after move");
            let slot = slots
                .get(indices::SlotIndex::fixed::<0>())
                .expect("active slot");
            assert!(std::ptr::eq(slot.key().context(), moved.context()));
            let lease = slot.key().arm(NonZero::<u64>::MIN).expect("idle slot");
            drop(lease);
        }
        assert_eq!(drops.get(), 0);
        Tracker::begin()
    };
    let freed = release.finish();
    assert_eq!(drops.get(), 1);
    assert_eq!(freed, [0, 0, 1, 8 * mem::size_of::<usize>()].into());
}

#[test]
fn keeps_generation() {
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(());
    let first = NonZero::<u64>::MIN;
    let next = first.saturating_add(1);
    let signal = NonZero::<u8>::MIN;
    let old = {
        let slots = owner
            .try_slots(NonZero::<u32>::MIN)
            .expect("first allocation");
        let slot = slots
            .get(indices::SlotIndex::fixed::<0>())
            .expect("active slot");
        let key = slot.key();
        let lease = key.arm(first).expect("idle slot");
        lease.release_with(signal, |context, value| {
            assert_eq!(*context, ());
            assert_eq!(value, first);
        });
        key
    };
    let (result, alloc) = Tracker::measure(|| owner.try_slots(NonZero::<u32>::MIN));
    assert_eq!(alloc, [0, 0, 0, 0]);
    let slots = result.expect("reuse group");
    let slot = slots
        .get(indices::SlotIndex::fixed::<0>())
        .expect("active slot");
    let lease = slot.key().arm(next).expect("idle next generation");
    lease.release_with(signal, |context, value| {
        assert_eq!(*context, ());
        assert_eq!(value, next);
    });
    let stale = error::Stale {
        expected: first,
        observed: error::Generation::Active(next),
    };
    assert!(matches!(old.arm(first), Err(error::Arm::Stale(cause)) if cause == stale));
    assert_eq!(slot.take(), Some(signal));
    assert_eq!(slot.take(), None);
}

#[test]
fn release_order_pins() {
    for detached in [false, true] {
        for release in [false, true] {
            let observed = Cell::new(None);
            let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(((), &observed));
            let capacity = NonZero::<u32>::MIN.saturating_add(1);
            let [first, second] = {
                let slots = owner.try_slots(capacity).expect("initial allocation");
                [0_u16, 1].map(|index| {
                    let slot = slots
                        .get(indices::SlotIndex::from(index))
                        .expect("active slot");
                    let lease = slot.key().arm(NonZero::<u64>::MIN).expect("idle slot");
                    if detached {
                        slot.detach();
                    }
                    lease
                })
            };
            let (result, alloc) = Tracker::measure(|| owner.try_slots(capacity));
            let busy = result.expect("two held leases require allocation");
            assert_eq!(alloc, [1, 10 * mem::size_of::<usize>(), 0, 0]);
            if release {
                first.release_with(NonZero::<u8>::MIN, record);
            } else {
                drop(first);
            }
            let (result, alloc) = Tracker::measure(|| owner.try_slots(capacity));
            let other = result.expect("remaining held lease requires allocation");
            assert_eq!(alloc, [1, 10 * mem::size_of::<usize>(), 0, 0]);
            if release {
                drop(second);
            } else {
                second.release_with(NonZero::<u8>::MIN, record);
            }
            let (result, alloc) = Tracker::measure(|| owner.try_slots(capacity));
            let slots = result.expect("fully released group is reusable");
            assert_eq!(alloc, [0, 0, 0, 0]);
            assert_eq!(slots.iter().count(), 2);
            drop((slots, busy, other));
            assert_eq!(observed.get(), None);
        }
    }
}

#[test]
fn release_reenters() {
    let owner = storage::Owner::<NonZero<u64>, NonZero<u8>, _>::new(());
    let capacity = NonZero::<u32>::MIN.saturating_add(1);
    let slots = owner.try_slots(capacity).expect("credit storage");
    let first = slots
        .get(indices::SlotIndex::fixed::<0>())
        .expect("first slot")
        .key();
    let lease = first.arm(NonZero::<u64>::MIN).expect("first idle slot");
    let held = slots
        .get(indices::SlotIndex::fixed::<1>())
        .expect("second slot")
        .key()
        .arm(NonZero::<u64>::MIN)
        .expect("second idle slot");
    lease.release_with(NonZero::<u8>::MIN, |_, _| {
        drop(slots);
        let (busy, activity) = Tracker::measure(|| owner.try_slots(capacity));
        let busy = busy.expect("the surviving lease pins its retired group");
        assert_eq!(activity[0], 1);
        drop(held);
        let (reused, activity) = Tracker::measure(|| owner.try_slots(capacity));
        let reused = reused.expect("the final lease releases its exact group");
        assert_eq!(activity, [0; 4]);
        let expected = error::Stale {
            expected: NonZero::<u64>::MIN,
            observed: error::Generation::Active(NonZero::<u64>::MIN.saturating_add(1)),
        };
        assert!(matches!(first.arm(NonZero::<u64>::MIN), Err(error::Arm::Stale(cause)) if cause == expected));
        drop((reused, busy));
    });
}
