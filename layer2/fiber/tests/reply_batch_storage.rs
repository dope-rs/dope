use dope_fiber::reply::{Arena, layout, usage};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn allocate_batch_storage() {
    let layout = layout::Layout::single(
        layout::Entries::new(4),
        layout::Limit::new(
            layout::Data::new(4),
            usage::Bytes::new(4),
            usage::Values::new(4),
        ),
    )
    .expect("reply layout");
    region::Token::scope(|token| {
        let proof = token.proof();
        let (plain, plainmem) = Tracker::activity(|| Arena::<()>::try_with(layout, proof, |_| ()));
        plain.expect("plain arena");
        let (batch, batchmem) =
            Tracker::activity(|| Arena::<()>::try_batches(layout, proof, |_, _| ()));
        batch.expect("batch arena");
        assert_eq!(batchmem.allocations - plainmem.allocations, 1);
        assert_eq!(
            batchmem.allocated_bytes - plainmem.allocated_bytes,
            4 * 7 * size_of::<usize>()
        );
    });
}
