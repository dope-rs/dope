use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

use dope_net::link::egress::StaticBytes;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::metadata;
use dope_net::link::egress::storage::Storage;
use dope_test::allocations_during;
use o3::buffer::Shared;
use o3::cell::RegionToken;

#[test]
fn oversized_single_stage_is_refused() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};

    RegionToken::scope(|mut token| {
        let storage = Storage::default();
        let mut arena = storage.arena::<SendBuffer, 32>(&token, 1);
        let queue = arena.queue();
        let deferred = DeferredEgress::new();
        let big = Shared::from(vec![0u8; 2 * 1024 * 1024]);
        assert!(!deferred.stage(&mut token, &queue, big, false));
        assert!(deferred.is_idle(&queue));
        assert!(deferred.stage(&mut token, &queue, Shared::copy_from_slice(b"hello"), false,));
        assert!(!deferred.is_idle(&queue));
    });
}

#[test]
fn static_payload_uses_entry_credit_without_resident_byte_credit() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};

    static BODY: [u8; 2 * 1024 * 1024] = [0; 2 * 1024 * 1024];
    RegionToken::scope(|mut token| {
        let storage = Storage::default();
        let mut arena = storage.arena::<SendBuffer, 32>(&token, 1);
        let queue = arena.queue();
        let deferred = DeferredEgress::new();
        assert!(deferred.stage_buffer(&mut token, &queue, SendBuffer::Static(&BODY), false));
        assert_eq!(queue.total_bytes(), BODY.len());
    });
}

#[test]
fn partial_ack_keeps_retained_buffer_credit_until_release() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(2, 4);
        let mut arena = storage.arena::<Shared, 8>(&token, 1);
        let mut queue = arena.queue();

        queue
            .try_enqueue(&mut token, Shared::copy_from_slice(b"abcd"))
            .unwrap();
        assert!(queue.try_ack(&mut token, 2));
        assert!(
            queue
                .try_enqueue(&mut token, Shared::copy_from_slice(b"xy"))
                .is_err()
        );
        assert!(queue.try_ack(&mut token, 2));
        queue
            .try_enqueue(&mut token, Shared::copy_from_slice(b"xy"))
            .unwrap();
    });
}

#[test]
fn overflowed_stage_refuses_later_writes() {
    RegionToken::scope(|mut token| {
        let storage = Storage::default();
        let mut arena = storage.arena::<Shared, 8>(&token, 1);
        let mut queue = arena.queue();
        let mut stage = queue.wire_stage(&mut token);
        stage.extend_from_slice(&vec![0; 64 * 1024 + 1]);
        assert!(stage.overflowed());
        stage.push(1);
        assert_eq!(stage.len(), 0);
        assert_eq!(stage.commit(), 0);
        assert_eq!(queue.total_bytes(), 0);
    });
}

#[test]
fn wire_spans_survive_compaction_after_prefix_ack() {
    const CAPACITY: usize = 64 * 1024;

    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::shared(8, CAPACITY as u32));
        let mut arena = storage.arena::<Shared, 8>(&token, 1);
        let mut queue = arena.queue();
        {
            let mut stage = queue.wire_stage(&mut token);
            stage.push(b'a');
            assert_eq!(stage.commit(), 1);
        }
        {
            let mut stage = queue.wire_stage(&mut token);
            stage.extend_from_slice(&vec![b'b'; CAPACITY - 1]);
            assert_eq!(stage.commit(), CAPACITY - 1);
        }
        assert!(queue.try_ack(&mut token, 1));
        {
            let mut stage = queue.wire_stage(&mut token);
            stage.push(b'c');
            assert_eq!(stage.commit(), 1);
        }
        assert_eq!(queue.total_bytes(), CAPACITY);
        assert!(queue.try_ack(&mut token, CAPACITY));
        assert_eq!(queue.total_bytes(), 0);
    });
}

#[test]
fn non_aligned_wire_budget_rounds_up_instead_of_disappearing() {
    const CAPACITY: u32 = o3::buffer::BLOCK_CAPACITY;

    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::shared(2, CAPACITY + 1));
        let mut arena = storage.arena::<Shared, 8>(&token, 2);
        for lane in 0..2 {
            let mut queue = arena.queue_for(lane);
            let mut stage = queue.wire_stage(&mut token);
            stage.push(lane as u8);
            assert_eq!(stage.commit(), 1);
        }
    });
}

#[test]
fn wire_stage_prepare_and_ack_allocate_nothing() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::shared(2, 64 * 1024));
        let mut arena = storage.arena::<Shared, 8>(&token, 1);
        let mut queue = arena.queue();
        let (allocations, bytes) = allocations_during(|| {
            let mut stage = queue.wire_stage(&mut token);
            stage.extend_from_slice(b"wire");
            assert_eq!(stage.commit(), 4);
            assert!(queue.try_ack(&mut token, 4));
        });
        assert_eq!((allocations, bytes), (0, 0));
    });
}

struct Lease<'a> {
    dropped: &'a Cell<bool>,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[test]
fn generic_entry_releases_with_arena() {
    let dropped = Cell::new(false);
    RegionToken::scope(|mut token| {
        let storage = Storage::default();
        let mut arena = storage.arena::<StaticBytes<Lease<'_>>, 8>(&token, 1);
        {
            let queue = arena.queue();
            assert!(
                queue
                    .try_enqueue(
                        &mut token,
                        StaticBytes::new(b"lease", Lease { dropped: &dropped },),
                    )
                    .is_ok()
            );
        }
        drop(arena);
    });
    assert!(dropped.get());
}

#[test]
fn direct_queue_credit_failure_reclaims_the_reserved_node() {
    RegionToken::scope(|mut token| {
        let arena = metadata::Arena::with_config(&token, Config::shared(1, 1), 1);
        let queue = arena.queue(0);
        assert!(queue.try_push_back(&mut token, &b"xx"[..], 2).is_err());
        assert!(queue.try_push_back(&mut token, &b"x"[..], 1).is_ok());
    });
}

#[test]
fn lane_credit_queue_hot_path_allocates_nothing() {
    RegionToken::scope(|mut token| {
        let arena =
            metadata::Arena::with_config(&token, Config::partitioned(2, 2, 2, 2).unwrap(), 2);
        let first = arena.queue(0);
        let second = arena.queue(1);
        let (allocations, bytes) = allocations_during(|| {
            first.try_push_back(&mut token, &b"a"[..], 1).unwrap();
            second.try_push_back(&mut token, &b"b"[..], 1).unwrap();
            let (value, front) = first.take_front(&mut token).unwrap();
            assert_eq!(value, b"a");
            front.release();
            arena.clear(1, &mut token);
        });
        assert_eq!((allocations, bytes), (0, 0));
    });
}

#[test]
fn detached_values_release_the_queue_on_drop() {
    RegionToken::scope(|mut token| {
        let arena = metadata::Arena::with_config(&token, Config::shared(2, 2), 1);
        let queue = arena.queue(0);
        queue.try_push_back(&mut token, b"old", 1).unwrap();
        let detached = queue.detach_all(&mut token);
        drop(detached);
        queue.try_push_back(&mut token, b"new", 1).unwrap();
        let (value, front) = queue.take_front(&mut token).expect("new value");
        assert_eq!(value, b"new");
        front.release();
        assert!(queue.is_empty());
    });
}

#[test]
fn empty_buffers_consume_neither_entry_nor_byte_credit() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(1, 1);
        let mut arena = storage.arena::<&'static [u8], 8>(&token, 1);
        let queue = arena.queue();
        for _ in 0..8 {
            queue.try_enqueue(&mut token, b"").unwrap();
        }
        queue.try_enqueue(&mut token, b"x").unwrap();
        assert_eq!(queue.total_bytes(), 1);
    });
}

#[test]
fn connection_lanes_keep_reserve_and_share_surplus() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::partitioned(2, 2, 4, 4).unwrap());
        let mut arena = storage.arena::<&'static [u8], 8>(&token, 2);
        {
            let first = arena.queue_for(0);
            first.try_enqueue(&mut token, b"aa").unwrap();
            first.try_enqueue(&mut token, b"bb").unwrap();
            first.try_enqueue(&mut token, b"cc").unwrap();
            assert!(first.try_enqueue(&mut token, b"dd").is_err());
        }
        arena.queue_for(1).try_enqueue(&mut token, b"zz").unwrap();
    });
}

#[test]
fn reserve_remainder_stays_with_a_lane() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::partitioned(3, 0, 3, 0).unwrap());
        let mut arena = storage.arena::<&'static [u8], 8>(&token, 2);
        {
            let second = arena.queue_for(1);
            second.try_enqueue(&mut token, b"a").unwrap();
            assert!(second.try_enqueue(&mut token, b"b").is_err());
        }
        let first = arena.queue_for(0);
        first.try_enqueue(&mut token, b"c").unwrap();
        first.try_enqueue(&mut token, b"d").unwrap();
    });
}

#[test]
fn clearing_a_lane_releases_its_credit_for_reuse() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(1, 1);
        let mut arena = storage.arena::<&'static [u8], 8>(&token, 1);
        arena.queue_for(0).try_enqueue(&mut token, b"x").unwrap();
        assert!(arena.clear(&mut token, 0));
        arena.queue_for(0).try_enqueue(&mut token, b"x").unwrap();
    });
}

struct PanicLease<'a> {
    drops: &'a Cell<usize>,
    panicked: &'a Cell<bool>,
    panic: bool,
}

impl Drop for PanicLease<'_> {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        assert!(
            !self.panic || self.panicked.replace(true),
            "egress payload drop panic"
        );
    }
}

#[test]
fn payload_drop_panic_reclaims_the_remaining_chain() {
    let drops = Cell::new(0);
    let panicked = Cell::new(false);
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(3, 3);
        let mut arena = storage.arena::<StaticBytes<PanicLease<'_>>, 8>(&token, 1);
        {
            let queue = arena.queue();
            for panic in [true, false, false] {
                assert!(
                    queue
                        .try_enqueue(
                            &mut token,
                            StaticBytes::new(
                                b"x",
                                PanicLease {
                                    drops: &drops,
                                    panicked: &panicked,
                                    panic,
                                },
                            ),
                        )
                        .is_ok()
                );
            }
        }
        assert!(catch_unwind(AssertUnwindSafe(|| arena.clear(&mut token, 0))).is_err());
        assert_eq!(drops.get(), 3);
        let queue = arena.queue();
        for _ in 0..3 {
            assert!(
                queue
                    .try_enqueue(
                        &mut token,
                        StaticBytes::new(
                            b"x",
                            PanicLease {
                                drops: &drops,
                                panicked: &panicked,
                                panic: false,
                            },
                        ),
                    )
                    .is_ok()
            );
        }
        assert!(
            queue
                .try_enqueue(
                    &mut token,
                    StaticBytes::new(
                        b"x",
                        PanicLease {
                            drops: &drops,
                            panicked: &panicked,
                            panic: false,
                        },
                    ),
                )
                .is_err()
        );
    });
}

#[test]
fn wire_staging_uses_one_bounded_shared_slot() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::shared(4, 64 * 1024));
        let mut arena = storage.arena::<&'static [u8], 8>(&token, 2);
        {
            let mut first = arena.queue_for(0);
            let mut stage = first.wire_stage(&mut token);
            stage.extend_from_slice(b"first");
            assert_eq!(stage.commit(), 5);
        }
        {
            let mut second = arena.queue_for(1);
            let stage = second.wire_stage(&mut token);
            assert!(stage.overflowed());
            assert_eq!(stage.commit(), 0);
        }
        assert!(arena.clear(&mut token, 0));
        let mut second = arena.queue_for(1);
        let mut stage = second.wire_stage(&mut token);
        stage.extend_from_slice(b"second");
        assert_eq!(stage.commit(), 6);
    });
}

#[test]
fn copied_egress_is_packed_across_connection_lanes() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};

    const LANES: usize = 64;
    RegionToken::scope(|mut token| {
        let storage = Storage::with_config(Config::shared(LANES as u32, 64 * 1024));
        let mut arena = storage.arena::<SendBuffer, 32>(&token, LANES);
        let mut queues: Vec<_> = (0..LANES).map(|_| DeferredEgress::new_for()).collect();
        let response = [b'x'; 202];
        for (lane, deferred) in queues.iter_mut().enumerate() {
            assert!(deferred.stage_copy(&mut token, &mut arena.queue_for(lane), &response, false,));
        }
        assert!(
            queues
                .iter()
                .enumerate()
                .all(|(lane, deferred)| { arena.bytes(lane) != 0 && !deferred.close_after() })
        );
    });
}

#[test]
fn split_batch_failure_rolls_back_every_entry_and_byte() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(1, 64);
        let mut arena = storage.arena::<Shared, 8>(&token, 1);
        let queue = arena.queue();
        assert!(!queue.try_enqueue_pair(
            &mut token,
            Shared::copy_from_slice(b"header"),
            Some(Shared::copy_from_slice(b"body")),
        ));
        assert_eq!(queue.total_bytes(), 0);
        queue
            .try_enqueue(&mut token, Shared::copy_from_slice(b"after"))
            .unwrap();
        assert_eq!(queue.total_bytes(), 5);
    });
}

#[test]
fn one_queue_can_use_the_global_byte_surplus() {
    RegionToken::scope(|mut token| {
        let storage = Storage::with_limits(2, 2 << 20);
        let mut arena = storage.arena::<Shared, 8>(&token, 2);
        let first = arena.queue_for(0);
        let payload = Shared::from(vec![1; 1536 << 10]);
        first.try_enqueue(&mut token, payload).unwrap();
        assert_eq!(first.total_bytes(), 1536 << 10);
    });
}
