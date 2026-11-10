use std::{num::NonZero, pin};

use dope::core::driver::schedule::ready::task::owner;
use dope_fiber::{
    abi::Ready,
    task::storage::{
        fixed::{
            Slab,
            local::Removal,
            schedule::{
                Pool,
                queue::{Owned, PrepareError},
            },
        },
        scope::Scope,
    },
};
use dope_test::alloc::Tracker;
use o3::{
    alloc::Error,
    depot::{arena::slab::storage, capacity::Count},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn direct_capacity() {
    type Slots = Slab<'static, u8, { u32::MAX as usize }, (), storage::Heap>;
    for count in [0, 1, 16385] {
        let (slots, allocated) = Tracker::usage(|| Slots::try_new_with_capacity(Count::new(count)));
        assert_eq!(allocated.0, usize::from(count != 0));
        let mut slots = pin::pin!(slots.expect("direct slots"));
        Scope::with(slots.as_mut(), |mut slots| {
            let mut ids = Vec::with_capacity(count as usize);
            let activity = Tracker::during(|| {
                assert_eq!(slots.admit().is_some(), count != 0);
                for _ in 0..count {
                    ids.push(slots.admit().expect("physical vacancy").insert(7));
                }
                assert!(slots.admit().is_none());
                assert!(slots.insert(9).is_none());
                for id in ids.drain(..) {
                    let mut entry = slots.entry(id).ok().expect("occupied slot");
                    assert_eq!(*entry.get(), 7);
                    entry.remove();
                }
                if count != 0 {
                    let id = slots.insert(9).expect("returned capacity");
                    assert!(slots.remove(id) == Removal::Removed);
                }
            });
            assert_eq!(activity, (0, 0));
        });
    }
}

#[test]
fn construction_releases_partial_storage() {
    let owners = owner::Capacity::new(NonZero::<u32>::MIN);
    for fail_after in [0, 1] {
        let (result, activity) = Tracker::activity(|| {
            Tracker::fail_after(fail_after, || {
                Pool::<'static, Ready<()>, (), (), Owned<'static, ()>>::try_owned(
                    Count::new(2),
                    owners,
                )
            })
        });
        assert!(matches!(
            (fail_after, result),
            (0, Err(PrepareError::Routes(Error::Exhausted)))
                | (1, Err(PrepareError::Queue(Error::Exhausted)))
        ));
        assert_eq!(activity.allocations, fail_after);
        assert_eq!(activity.deallocations, fail_after);
        assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
    }
}
