use dope_core::io::transfer;
use dope_net::link::egress::{self, data};
use o3::buffer::{self, resident, storage};

#[test]
fn receive_owner_moves() {
    let src = (storage::Shared::try_copy(b"receive-echo"))
        .expect("the echo fixture must fit shared storage");
    let ptr = src.as_slice().as_ptr();
    let egress = resident::Shared::from(src);
    let bytes = buffer::Residency::resident(&egress);
    assert_eq!(egress::Charge::of(&egress).resident().get(), bytes);

    let mut chunks =
        (data::Chunks::from_offset(&egress, data::Offset::from_len(transfer::Len::fixed::<3>())))
            .expect("the echo chunk offset must be inside the buffer");
    let chunk = (chunks.take(transfer::NonZero::fixed_size::<4>()))
        .expect("four echo bytes must remain after the offset");
    assert_eq!(chunk.bytes(), b"eive");
    assert_eq!(chunk.bytes().as_ptr(), ptr.wrapping_add(3));
    let chunk = (chunks.take(transfer::NonZero::fixed_size::<5>()))
        .expect("the five-byte echo suffix must remain");
    assert_eq!(chunk.bytes(), b"-echo");
    assert!(chunks.take(transfer::NonZero::fixed_size::<1>()).is_none());
}
