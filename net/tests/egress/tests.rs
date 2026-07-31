use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use dope_net::link::egress::arena::Arena;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::metadata::MetadataArena;
use dope_net::link::egress::storage::Storage;
use dope_test::allocations_during;
use o3::buffer::Shared;

#[test]
fn oversized_single_stage_is_refused() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};
    let storage = Storage::default();
    let mut arena = storage.arena::<SendBuffer, 32>(1);
    let queue = arena.queue();
    let d = DeferredEgress::new();
    let big = Shared::from(vec![0u8; 2 * 1024 * 1024]);
    assert!(
        !d.stage(&queue, big, false),
        "a single buffer exceeding the 1MB egress cap must be refused (not pushed unconditionally)"
    );
    assert!(
        d.is_idle(&queue),
        "a refused stage must not enqueue, so the cap is a real bound"
    );
    assert!(d.stage(&queue, Shared::copy_from_slice(b"hello"), false));
    assert!(!d.is_idle(&queue));
}

#[test]
fn static_payload_uses_entry_credit_without_resident_byte_credit() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};

    static BODY: [u8; 2 * 1024 * 1024] = [0; 2 * 1024 * 1024];
    let storage = Storage::default();
    let mut arena = storage.arena::<SendBuffer, 32>(1);
    let mut queue = arena.queue();
    let mut deferred = DeferredEgress::new();
    assert!(deferred.stage_buffer(&queue, SendBuffer::Static(&BODY), false));
    assert_eq!(
        deferred.prepare_send(&mut queue, usize::MAX).bytes(),
        BODY.len()
    );
}

#[test]
fn partial_ack_keeps_retained_buffer_credit_until_release() {
    let storage = Storage::with_limits(2, 4);
    let mut arena = storage.arena::<Shared, 8>(1);
    let mut queue = arena.queue();

    queue.try_enqueue(Shared::copy_from_slice(b"abcd")).unwrap();
    assert!(queue.try_ack(2));

    assert!(
        queue.try_enqueue(Shared::copy_from_slice(b"xy")).is_err(),
        "a partial ACK must not release credit while the backing buffer is retained"
    );

    assert!(queue.try_ack(2));
    queue.try_enqueue(Shared::copy_from_slice(b"xy")).unwrap();
}

#[test]
fn overflowed_stage_refuses_later_writes() {
    let storage = Storage::default();
    let mut arena = storage.arena::<Shared, 8>(1);
    let mut q = arena.queue();
    let mut stage = q.wire_stage();
    stage.extend_from_slice(&vec![0; 64 * 1024 + 1]);
    assert!(stage.overflowed());
    stage.push(1);
    assert_eq!(stage.len(), 0);
    assert_eq!(stage.commit(), 0);
    assert_eq!(q.total_bytes(), 0);
}

struct Lease<'a> {
    bytes: &'static [u8],
    dropped: &'a Cell<bool>,
}

impl AsRef<[u8]> for Lease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[test]
fn generic_entry_releases_with_arena() {
    let dropped = Cell::new(false);
    let storage = Storage::default();
    let mut arena = storage.arena::<Lease<'_>, 8>(1);
    {
        let queue = arena.queue();
        assert!(
            queue
                .try_enqueue(Lease {
                    bytes: b"lease",
                    dropped: &dropped,
                })
                .is_ok()
        );
    }
    drop(arena);
    assert!(dropped.get());
}

struct ReentrantAsRef<'a> {
    bytes: [u8; 5],
    arena: &'a Cell<Option<&'a Arena<'a, ReentrantAsRef<'a>, 8>>>,
    reentered: Rc<Cell<bool>>,
}

impl AsRef<[u8]> for ReentrantAsRef<'_> {
    fn as_ref(&self) -> &[u8] {
        if !self.reentered.replace(true)
            && let Some(arena) = self.arena.get()
        {
            assert!(
                arena
                    .try_enqueue(
                        0,
                        Self {
                            bytes: *b"inner",
                            arena: self.arena,
                            reentered: self.reentered.clone(),
                        },
                    )
                    .is_ok()
            );
        }
        &self.bytes
    }
}

#[test]
fn as_ref_reentry_never_observes_the_prepared_node() {
    let slot: &'static Cell<Option<&'static Arena<'static, ReentrantAsRef<'static>, 8>>> =
        Box::leak(Box::new(Cell::new(None)));
    let storage = Box::leak(Box::new(Storage::with_limits(4, 64)));
    let arena: &'static Arena<'static, ReentrantAsRef<'static>, 8> =
        Box::leak(Box::new(storage.arena(1)));
    slot.set(Some(arena));
    let reentered = Rc::new(Cell::new(false));
    assert!(
        arena
            .try_enqueue(
                0,
                ReentrantAsRef {
                    bytes: *b"outer",
                    arena: slot,
                    reentered: reentered.clone(),
                },
            )
            .is_ok()
    );
    assert!(reentered.get());
    assert_eq!(arena.bytes(0), 10);
}

struct PanicAsRef {
    panic: bool,
}

impl AsRef<[u8]> for PanicAsRef {
    fn as_ref(&self) -> &[u8] {
        assert!(!self.panic, "egress as_ref panic");
        b"x"
    }
}

#[test]
fn as_ref_panic_reclaims_the_reserved_node() {
    let storage = Storage::with_limits(1, 1);
    let mut arena = storage.arena::<PanicAsRef, 8>(1);
    let queue = arena.queue();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            drop(queue.try_enqueue(PanicAsRef { panic: true }));
        }))
        .is_err()
    );
    assert!(queue.try_enqueue(PanicAsRef { panic: false }).is_ok());
}

#[test]
fn direct_queue_credit_failure_reclaims_the_reserved_node() {
    let arena: MetadataArena<&'static [u8]> = MetadataArena::with_config(Config::shared(1, 1), 1);
    let queue = arena.queue(0);
    assert!(queue.try_push_back(&b"xx"[..], 2).is_err());
    assert!(queue.try_push_back(&b"x"[..], 1).is_ok());
}

#[test]
fn lane_credit_queue_hot_path_allocates_nothing() {
    let arena: MetadataArena<&'static [u8]> =
        MetadataArena::with_config(Config::partitioned(2, 2, 2, 2).unwrap(), 2);
    let first = arena.queue(0);
    let second = arena.queue(1);

    let (allocations, bytes) = allocations_during(|| {
        first.try_push_back(&b"a"[..], 1).unwrap();
        second.try_push_back(&b"b"[..], 1).unwrap();
        let (value, front) = first.take_front().unwrap();
        assert_eq!(value, b"a");
        front.release();
        arena.clear(1);
    });

    assert_eq!((allocations, bytes), (0, 0));
}

#[test]
fn detached_values_do_not_close_the_queue() {
    let arena: MetadataArena<&'static [u8]> = MetadataArena::with_config(Config::shared(2, 2), 1);
    let queue = arena.queue(0);
    queue.try_push_back(b"old", 1).unwrap();

    let detached = queue.detach_all();
    queue.try_push_back(b"new", 1).unwrap();
    drop(detached);

    let (value, front) = queue.take_front().expect("new value");
    assert_eq!(value, b"new");
    front.release();
    assert!(queue.is_empty());
}

#[test]
fn empty_buffers_consume_neither_entry_nor_byte_credit() {
    let storage = Storage::with_limits(1, 1);
    let mut arena: Arena<'_, &'static [u8], 8> = storage.arena(1);
    let queue = arena.queue();
    for _ in 0..8 {
        queue.try_enqueue(b"").unwrap();
    }
    queue.try_enqueue(b"x").unwrap();
    assert_eq!(queue.total_bytes(), 1);
}

#[test]
fn connection_lanes_keep_reserve_and_share_surplus() {
    let storage = Storage::with_config(Config::partitioned(2, 2, 4, 4).unwrap());
    let mut arena: Arena<'_, &'static [u8], 8> = storage.arena(2);
    {
        let first = arena.queue_for(0);
        first.try_enqueue(b"aa").unwrap();
        first.try_enqueue(b"bb").unwrap();
        first.try_enqueue(b"cc").unwrap();
        assert!(first.try_enqueue(b"dd").is_err());
    }
    arena.queue_for(1).try_enqueue(b"zz").unwrap();
}

#[test]
fn reserve_remainder_stays_with_a_lane() {
    let storage = Storage::with_config(Config::partitioned(3, 0, 3, 0).unwrap());
    let mut arena: Arena<'_, &'static [u8], 8> = storage.arena(2);
    {
        let second = arena.queue_for(1);
        second.try_enqueue(b"a").unwrap();
        assert!(second.try_enqueue(b"b").is_err());
    }
    let first = arena.queue_for(0);
    first.try_enqueue(b"c").unwrap();
    first.try_enqueue(b"d").unwrap();
}

#[test]
fn clearing_a_lane_releases_its_credit_for_reuse() {
    let storage = Storage::with_limits(1, 1);
    let mut arena: Arena<'_, &'static [u8], 8> = storage.arena(1);
    arena.queue_for(0).try_enqueue(b"x").unwrap();
    arena.clear(0);
    let second = arena.queue_for(0);
    second.try_enqueue(b"x").unwrap();
}

struct PanicLease<'a> {
    bytes: &'static [u8],
    drops: &'a Cell<usize>,
    panicked: &'a Cell<bool>,
    panic: bool,
}

impl AsRef<[u8]> for PanicLease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
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
    let storage = Storage::with_limits(3, 3);
    let mut arena = storage.arena::<PanicLease<'_>, 8>(1);
    {
        let queue = arena.queue();
        for panic in [true, false, false] {
            assert!(
                queue
                    .try_enqueue(PanicLease {
                        bytes: b"x",
                        drops: &drops,
                        panicked: &panicked,
                        panic,
                    })
                    .is_ok()
            );
        }
    }
    assert!(catch_unwind(AssertUnwindSafe(|| arena.clear(0))).is_err());
    assert_eq!(drops.get(), 3);

    let queue = arena.queue();
    for _ in 0..3 {
        assert!(
            queue
                .try_enqueue(PanicLease {
                    bytes: b"x",
                    drops: &drops,
                    panicked: &panicked,
                    panic: false,
                })
                .is_ok()
        );
    }
    assert!(
        queue
            .try_enqueue(PanicLease {
                bytes: b"x",
                drops: &drops,
                panicked: &panicked,
                panic: false,
            })
            .is_err()
    );
}

#[test]
fn wire_staging_uses_one_bounded_shared_slot() {
    let storage = Storage::with_config(Config::shared(4, 64 * 1024));
    let mut arena: Arena<'_, &'static [u8], 8> = storage.arena(2);
    {
        let mut first = arena.queue_for(0);
        let mut stage = first.wire_stage();
        stage.extend_from_slice(b"first");
        assert_eq!(stage.commit(), 5);
    }
    {
        let mut second = arena.queue_for(1);
        let stage = second.wire_stage();
        assert!(stage.overflowed());
        assert_eq!(stage.commit(), 0);
    }
    arena.clear(0);
    let mut second = arena.queue_for(1);
    let mut stage = second.wire_stage();
    stage.extend_from_slice(b"second");
    assert_eq!(stage.commit(), 6);
}

#[test]
fn copied_egress_is_packed_across_connection_lanes() {
    use dope_net::link::slot::DeferredEgress;

    const LANES: usize = 64;
    let storage = Storage::with_config(Config::shared(LANES as u32, 64 * 1024));
    let mut arena: Arena<'_, dope_net::link::slot::SendBuffer> = storage.arena(LANES);
    let mut queues: Vec<_> = (0..LANES).map(|_| DeferredEgress::new_for()).collect();
    let response = [b'x'; 202];

    for (lane, deferred) in queues.iter_mut().enumerate() {
        assert!(deferred.stage_copy(&mut arena.queue_for(lane), &response, false));
    }
    assert!(
        queues
            .iter()
            .enumerate()
            .all(|(lane, deferred)| arena.bytes(lane) != 0 && !deferred.close_after())
    );
}

#[test]
fn split_batch_failure_rolls_back_every_entry_and_byte() {
    let storage = Storage::with_limits(1, 64);
    let mut arena = storage.arena::<Shared, 8>(1);
    let queue = arena.queue();
    assert!(!queue.try_enqueue_pair(
        Shared::copy_from_slice(b"header"),
        Some(Shared::copy_from_slice(b"body")),
    ));
    assert_eq!(queue.total_bytes(), 0);
    queue
        .try_enqueue(Shared::copy_from_slice(b"after"))
        .unwrap();
    assert_eq!(queue.total_bytes(), 5);
}

#[test]
fn one_queue_can_use_the_global_byte_surplus() {
    let storage = Storage::with_limits(2, 2 << 20);
    let mut arena = storage.arena::<Shared, 8>(2);
    let first = arena.queue_for(0);
    let payload = Shared::from(vec![1; 1536 << 10]);
    first.try_enqueue(payload).unwrap();
    assert_eq!(first.total_bytes(), 1536 << 10);
}
