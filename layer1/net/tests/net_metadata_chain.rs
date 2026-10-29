use std::num::NonZero;

use dope_net::link::egress::{
    self,
    metadata::{Lane, arena, batch},
};
use dope_test::alloc::Tracker;
use o3::{alloc, base::cell::region};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn allocates_lanes_once() {
    for count in [1, 3, 257] {
        let lanes = NonZero::new(count).expect("positive lane count");
        let ((), activity) = Tracker::measure(|| {
            let (store, alloc) =
                Tracker::measure(|| arena::Store::try_new(egress::Config::DEFAULT, lanes));
            assert!(store.is_ok(), "lane storage allocation");
            assert_eq!(alloc, [1, count * 24, 0, 0]);
        });
        assert_eq!(activity, [1, count * 24, 1, count * 24]);
    }
}

#[test]
fn retains_alloc_failure() {
    let failure = Tracker::fail_after(0, || {
        arena::Store::try_new(egress::Config::DEFAULT, NonZero::<usize>::MIN)
    });
    assert!(matches!(
        failure,
        Err(arena::CreateError::Allocation(alloc::Error::Exhausted))
    ));
    assert!(arena::Store::try_new(egress::Config::DEFAULT, NonZero::<usize>::MIN).is_ok());
}

#[test]
fn partitions_protected() {
    region::Token::scope(|token| {
        let mut store = arena::Store::try_new(
            egress::Config::DEFAULT,
            NonZero::new(3).expect("three lanes"),
        )
        .expect("lane allocation");
        let arena = arena::Arena::<&[u8]>::try_new(&token, &mut store).expect("metadata arena");
        for (index, (entries, resident)) in [(2731, 699051), (2731, 699051), (2730, 699050)]
            .into_iter()
            .enumerate()
        {
            let lane = Lane::at(NonZero::<u32>::MIN.saturating_add(index as u32));
            let slot = arena.get(lane).expect("configured lane");
            let queue = slot.queue();
            for (entries, resident, fits) in [
                (entries, resident, true),
                (entries + 1, resident, false),
                (entries, resident + 1, false),
            ] {
                let plan = batch::Plan::new(
                    egress::Entries::new(entries),
                    egress::Resident::new(resident),
                )
                .expect("nonempty credit");
                assert_eq!(queue.can_acquire(plan), fits);
            }
        }
    });
}
