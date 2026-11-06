use dope_fiber::reply::{Arena, LaneIndex, layout, recv, recv::attach::Attach as _, usage};
use dope_test::alloc::Tracker;
use o3::base::cell::region;

pub mod common;
use common::storage::replies::{
    TakeBatch,
    support::{Counted, Drops, capacity},
};

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

#[test]
fn map_retires_consumed_rows() {
    for stop in [0, 2, 4] {
        let source = Drops::new();
        let output = Drops::new();
        region::Token::scope(|mut token| {
            let layout = layout::Layout::bounded(capacity(1, 1, 4, 4, 4)).expect("reply layout");
            Arena::try_batches(layout, token.proof(), |arena, batches| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                let mut reply = recv::Response::<_, TakeBatch>::with(batches);
                reply
                    .try_attach(&mut token, lane)
                    .expect("reply attachment");
                for _ in 0..4 {
                    assert!(
                        lane.select(&mut token)
                            .try_push(Counted::mixed(&source))
                            .is_ok()
                    );
                }
                lane.complete(&mut token);
                let batch = reply
                    .try_take(&mut token)
                    .expect("completed batch")
                    .expect("batch");
                let mut visited = 0;
                let result = Tracker::fail_after(usize::from(stop != 0), || {
                    batch.try_map(|row, slot| {
                        assert_eq!(source.count(), visited);
                        assert_eq!(row.bytes, 1);
                        if visited != 0 {
                            drop(
                                lane.try_reserve(usage::Usage::new(
                                    usage::Bytes::new(visited),
                                    usage::Values::new(visited),
                                ))
                                .expect("returned credit"),
                            );
                        }
                        visited += 1;
                        let written = slot.insert(Counted::mixed(&output));
                        if visited == stop && stop < 4 {
                            Err(7)
                        } else {
                            Ok(written)
                        }
                    })
                });
                assert_eq!(visited, stop);
                assert_eq!(source.count(), stop);
                match (stop, result) {
                    (0, Err(_)) | (2, Ok(Err(7))) => assert_eq!(output.count(), stop),
                    (4, Ok(Ok(values))) => {
                        assert_eq!(output.count(), 0);
                        assert_eq!(values.len(), 4);
                        drop(values);
                    }
                    _ => panic!("mapping result"),
                }
            })
            .expect("batch arena");
        });
        assert_eq!(source.count(), 4);
        assert_eq!(output.count(), stop);
    }
}
