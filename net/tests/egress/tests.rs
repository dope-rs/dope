use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::{Rc, Weak};

use dope_net::link::egress::arena::Arena;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::queue::Queue;
use o3::buffer::Shared;

fn queue<const IOV: usize, B: AsRef<[u8]>>() -> Queue<IOV, B> {
    Arena::default().queue()
}

#[test]
fn oversized_single_stage_is_refused() {
    use dope_net::link::slot::DeferredEgress;
    let d = DeferredEgress::new();
    let big = Shared::from(vec![0u8; 2 * 1024 * 1024]);
    assert!(
        !d.stage(big, false),
        "a single buffer exceeding the 1MB egress cap must be refused (not pushed unconditionally)"
    );
    assert!(
        d.is_idle(),
        "a refused stage must not enqueue, so the cap is a real bound"
    );
    assert!(d.stage(Shared::copy_from_slice(b"hello"), false));
    assert!(!d.is_idle());
}

#[test]
fn static_payload_uses_entry_credit_without_resident_byte_credit() {
    use dope_net::link::slot::{DeferredEgress, SendBuffer};

    static BODY: [u8; 2 * 1024 * 1024] = [0; 2 * 1024 * 1024];
    let mut deferred = DeferredEgress::new();
    assert!(deferred.stage_buffer(SendBuffer::Static(&BODY), false));
    assert_eq!(deferred.prepare_send(usize::MAX).bytes(), BODY.len());
}

#[test]
fn partial_ack_keeps_retained_buffer_credit_until_release() {
    let arena = Arena::with_limits(2, 4, 1);
    let queue = arena.queue::<8>();

    queue.try_enqueue(Shared::copy_from_slice(b"abcd")).unwrap();
    queue.ack(2);

    assert!(
        queue.try_enqueue(Shared::copy_from_slice(b"xy")).is_err(),
        "a partial ACK must not release credit while the backing buffer is retained"
    );

    queue.ack(2);
    queue.try_enqueue(Shared::copy_from_slice(b"xy")).unwrap();
}

#[test]
fn overflowed_stage_refuses_later_writes() {
    let mut q: Queue<8> = queue();
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
fn generic_entry_releases_with_queue() {
    let dropped = Cell::new(false);
    let queue: Queue<8, Lease<'_>> = queue();
    assert!(
        queue
            .try_enqueue(Lease {
                bytes: b"lease",
                dropped: &dropped,
            })
            .is_ok()
    );
    drop(queue);
    assert!(dropped.get());
}

struct ReentrantAsRef {
    bytes: [u8; 5],
    queue: Weak<Queue<8, ReentrantAsRef>>,
    reentered: Rc<Cell<bool>>,
}

impl AsRef<[u8]> for ReentrantAsRef {
    fn as_ref(&self) -> &[u8] {
        if !self.reentered.replace(true)
            && let Some(queue) = self.queue.upgrade()
        {
            assert!(
                queue
                    .try_enqueue(Self {
                        bytes: *b"inner",
                        queue: self.queue.clone(),
                        reentered: self.reentered.clone(),
                    })
                    .is_ok()
            );
        }
        &self.bytes
    }
}

#[test]
fn as_ref_reentry_never_observes_the_prepared_node() {
    let arena = Arena::with_limits(4, 64, 1);
    let queue = Rc::new(arena.queue::<8>());
    let reentered = Rc::new(Cell::new(false));
    assert!(
        queue
            .try_enqueue(ReentrantAsRef {
                bytes: *b"outer",
                queue: Rc::downgrade(&queue),
                reentered: reentered.clone(),
            })
            .is_ok()
    );
    assert!(reentered.get());
    let queue = Rc::try_unwrap(queue).ok().expect("queue still shared");
    assert_eq!(queue.total_bytes(), 10);
}

#[test]
fn empty_buffers_consume_neither_entry_nor_byte_credit() {
    let arena: Arena<&'static [u8]> = Arena::with_limits(1, 1, 1);
    let queue = arena.queue::<8>();
    for _ in 0..8 {
        queue.try_enqueue(b"").unwrap();
    }
    queue.try_enqueue(b"x").unwrap();
    assert_eq!(queue.total_bytes(), 1);
}

#[test]
fn connection_lanes_keep_reserve_and_share_surplus() {
    let arena: Arena<&'static [u8]> =
        Arena::with_config(Config::partitioned(2, 2, 4, 4).unwrap(), 2);
    let first = arena.queue_for::<8>(0);
    let second = arena.queue_for::<8>(1);
    first.try_enqueue(b"aa").unwrap();
    first.try_enqueue(b"bb").unwrap();
    first.try_enqueue(b"cc").unwrap();
    assert!(first.try_enqueue(b"dd").is_err());
    second.try_enqueue(b"zz").unwrap();
}

#[test]
fn reserve_remainder_stays_with_a_lane() {
    let arena: Arena<&'static [u8]> =
        Arena::with_config(Config::partitioned(3, 0, 3, 0).unwrap(), 2);
    let first = arena.queue_for::<8>(0);
    let second = arena.queue_for::<8>(1);
    second.try_enqueue(b"a").unwrap();
    assert!(second.try_enqueue(b"b").is_err());
    first.try_enqueue(b"c").unwrap();
    first.try_enqueue(b"d").unwrap();
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
    let arena = Arena::with_limits(3, 3, 1);
    let queue = arena.queue::<8>();
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
    assert!(catch_unwind(AssertUnwindSafe(|| drop(queue))).is_err());
    assert_eq!(drops.get(), 3);

    let queue = arena.queue::<8>();
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
    let arena: Arena<&'static [u8]> = Arena::with_config(Config::shared(4, 64 * 1024), 2);
    let mut first = arena.queue_for::<8>(0);
    let mut second = arena.queue_for::<8>(1);

    let mut stage = first.wire_stage();
    stage.extend_from_slice(b"first");
    assert_eq!(stage.commit(), 5);

    let stage = second.wire_stage();
    assert!(stage.overflowed());
    assert_eq!(stage.commit(), 0);

    drop(first);
    let mut stage = second.wire_stage();
    stage.extend_from_slice(b"second");
    assert_eq!(stage.commit(), 6);
}

#[test]
fn copied_egress_is_packed_across_connection_lanes() {
    use dope_net::link::slot::DeferredEgress;

    const LANES: usize = 64;
    let arena: Arena<dope_net::link::slot::SendBuffer> =
        Arena::with_config(Config::shared(LANES as u32, 64 * 1024), LANES);
    let mut queues: Vec<_> = (0..LANES)
        .map(|lane| DeferredEgress::new_for(&arena, lane))
        .collect();
    let response = [b'x'; 202];

    for queue in &mut queues {
        assert!(queue.stage_copy(&response, false));
    }
    assert!(queues.iter().all(|queue| !queue.is_idle()));
}

#[test]
fn split_batch_failure_rolls_back_every_entry_and_byte() {
    let arena = Arena::with_limits(1, 64, 1);
    let queue = arena.queue::<8>();
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
    let arena = Arena::with_limits(2, 2 << 20, 2);
    let first = arena.queue_for::<8>(0);
    let _second = arena.queue_for::<8>(1);
    let payload = Shared::from(vec![1; 1536 << 10]);
    first.try_enqueue(payload).unwrap();
    assert_eq!(first.total_bytes(), 1536 << 10);
}
