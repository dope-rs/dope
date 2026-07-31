use dope_net::wire::RuntimeLimits;
use dope_net::wire::buffered::{Buffered, ScratchPool};

#[test]
fn frozen_scratch_range_keeps_the_same_storage() {
    let buffers = Buffered::try_fixed(1, 64, 0, 1).expect("buffer pool");
    let mut scratch = buffers.try_acquire_scratch().expect("scratch");
    scratch
        .try_extend_from_slice(b"headpayloadtail")
        .expect("fill scratch");
    let expected = scratch.as_slice()[4..11].as_ptr();

    let frozen = scratch.freeze_range(4..11).expect("valid range");

    assert_eq!(frozen.as_slice(), b"payload");
    assert_eq!(
        frozen.as_slice().as_ptr(),
        expected,
        "freezing a range must transfer, not copy, its storage"
    );
}

#[test]
fn scratch_extra_is_an_exact_bounded_reserve() {
    let buffers =
        Buffered::try_for_runtime_with_scratch_extra(RuntimeLimits::new(2, 3, 64), 3, 4, 64, 0)
            .expect("buffer pools");
    let mut leases = Vec::new();
    while let Some(lease) = buffers.try_acquire_scratch() {
        leases.push(lease);
    }
    assert_eq!(leases.len(), 2 * 3 + 4);
}

#[test]
fn scratch_only_runtime_has_no_receive_leases() {
    let buffers = Buffered::try_scratch_for_runtime(RuntimeLimits::new(2, 9, 64), 3, 4, 64)
        .expect("scratch pool");
    assert!(buffers.try_acquire_recv().is_none());
    let mut scratch = Vec::new();
    while let Some(lease) = buffers.try_acquire_scratch() {
        scratch.push(lease);
    }
    assert_eq!(scratch.len(), 2 * 3 + 4);
}

#[test]
fn scratch_pool_exposes_exact_typed_layout() {
    let pool = ScratchPool::try_new(3, 96).expect("scratch pool");
    assert_eq!(pool.capacity(), 96);
    assert_eq!(pool.available(), 3);

    let first = pool.try_acquire().expect("first scratch");
    let second = pool.try_acquire().expect("second scratch");
    assert_eq!(pool.available(), 1);
    drop((first, second));
    assert_eq!(pool.available(), 3);
}
