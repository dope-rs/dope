use dope_net::wire::RecvTarget;
use dope_test::allocations_during;

#[test]
fn receive_target_reuses_capacity_without_prefill_or_allocation() {
    let mut buffer = Vec::with_capacity(8);
    buffer.extend_from_slice(b"stale");

    let allocation = allocations_during(|| {
        let mut target = RecvTarget::new(&mut buffer);
        assert_eq!(
            target.with_limit(3, |target| {
                assert_eq!(target.write_prefix(b"abcd"), 3);
            }),
            3
        );
        assert_eq!(target.write_prefix(b"efghij"), 5);
    });

    assert_eq!(allocation, (0, 0));
    assert_eq!(buffer, b"abcefghi");
}
