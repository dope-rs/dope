use dope_net::wire::{RuntimeLimits, pools::RuntimeBuffers};
use o3::buffer;

#[test]
fn runtime_policy_exposes_o3_lease_buffers_without_a_wrapper() {
    let pools = RuntimeBuffers::try_fixed(1, 64, 0, 1).expect("buffer pools");
    let mut scratch = pools.try_acquire_scratch().expect("scratch");
    scratch
        .try_extend(b"headpayloadtail")
        .expect("fill scratch");
    let expected = scratch.as_slice()[4..11].as_ptr();

    let frozen = scratch.freeze().get(4..11).expect("valid range");

    assert_eq!(frozen.as_slice(), b"payload");
    assert_eq!(
        frozen.as_slice().as_ptr(),
        expected,
        "freezing a range must transfer, not copy, its storage"
    );
}

#[test]
fn runtime_policy_calculates_an_exact_scratch_reserve() {
    let pools = RuntimeBuffers::try_for_runtime_with_scratch_extra(
        RuntimeLimits::new(2, 3, 64),
        3,
        4,
        64,
        0,
    )
    .expect("buffer pools");
    let mut leases = Vec::new();
    while let Some(lease) = pools.try_acquire_scratch() {
        leases.push(lease);
    }
    assert_eq!(leases.len(), 2 * 3 + 4);
}

#[test]
fn runtime_pool_slot_calculation_separates_local_retained_and_transient_slots() {
    let limits = RuntimeLimits::new(2, 3, 64);
    assert_eq!(RuntimeBuffers::slot_count(limits, 1, 3, 1), Some(6));
    assert_eq!(RuntimeBuffers::slot_count(limits, 0, 3, 1), Some(4));
    assert_eq!(
        RuntimeBuffers::slot_count(RuntimeLimits::new(usize::MAX, 0, 64), 1, 0, 1),
        None
    );
}

#[test]
fn scratch_only_runtime_keeps_receive_slots_absent() {
    let pools = RuntimeBuffers::try_scratch_for_runtime(RuntimeLimits::new(2, 9, 64), 3, 4, 64)
        .expect("scratch pool");
    assert!(pools.try_acquire_recv().is_none());
    let mut scratch = Vec::new();
    while let Some(lease) = pools.try_acquire_scratch() {
        scratch.push(lease);
    }
    assert_eq!(scratch.len(), 2 * 3 + 4);
}

#[test]
fn raw_o3_pool_retains_the_exact_layout_runtime_policy_requested() {
    let pool = buffer::Pool::try_new(3, 96).expect("scratch pool");
    assert_eq!(pool.capacity(), 96);
    assert_eq!(pool.available(), 3);

    let first = pool.try_acquire_buffer().expect("first scratch");
    let second = pool.try_acquire_buffer().expect("second scratch");
    assert_eq!(pool.available(), 1);
    drop((first, second));
    assert_eq!(pool.available(), 3);
}
