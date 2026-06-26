use dope::transport::link::egress::Queue;
use o3::buffer::Shared;

fn iov_bytes(q: &mut Queue<8>) -> Vec<u8> {
    let v = q.prepare_send(u32::MAX as usize);
    let mut out = Vec::new();
    for iov in v.iovs {
        let slice =
            // SAFETY: iov points into Shared buffers owned by the queue, alive for this borrow.
            unsafe { std::slice::from_raw_parts(iov.as_ptr(), iov.len()) };
        out.extend_from_slice(slice);
    }
    out
}

#[test]
fn back_to_back_push_preserves_all_chunks() {
    let mut q: Queue<8> = Queue::new();
    q.push(Shared::copy_from_slice(b"AAAA"));
    q.push(Shared::copy_from_slice(b"BBBB"));
    assert_eq!(q.total_bytes(), 8);
    assert_eq!(iov_bytes(&mut q), b"AAAABBBB");
}

#[test]
fn ack_partial_then_remaining_preserved() {
    let mut q: Queue<8> = Queue::new();
    q.push(Shared::copy_from_slice(b"AAAA"));
    q.push(Shared::copy_from_slice(b"BBBB"));
    q.ack(6);
    assert_eq!(q.total_bytes(), 2);
    assert_eq!(iov_bytes(&mut q), b"BB");
    q.ack(2);
    assert_eq!(q.total_bytes(), 0);
    assert!(iov_bytes(&mut q).is_empty());
}

#[test]
fn many_chunks_drain_in_order() {
    let mut q: Queue<8> = Queue::new();
    for i in 0..6u8 {
        q.push(Shared::copy_from_slice(&[b'0' + i; 3]));
    }
    assert_eq!(q.total_bytes(), 18);
    let mut acc = Vec::new();
    while q.total_bytes() > 0 {
        let chunk = iov_bytes(&mut q);
        assert!(!chunk.is_empty());
        let take = chunk.len().min(4);
        acc.extend_from_slice(&chunk[..take]);
        q.ack(take);
    }
    assert_eq!(acc, b"000111222333444555");
}

#[test]
fn oversized_single_stage_is_refused() {
    use dope::transport::link::DeferredEgress;
    let mut d = DeferredEgress::default();
    let big = Shared::copy_from_slice(&vec![0u8; 2 * 1024 * 1024]);
    assert!(
        !d.stage(big, false),
        "a single buffer exceeding the 1MB egress cap must be refused (not pushed unconditionally)"
    );
    assert!(
        d.is_idle(),
        "a refused stage must not enqueue, so the cap is a real bound"
    );
}

#[test]
fn within_cap_stage_succeeds() {
    use dope::transport::link::DeferredEgress;
    let mut d = DeferredEgress::default();
    assert!(d.stage(Shared::copy_from_slice(b"hello"), false));
    assert!(!d.is_idle());
}
