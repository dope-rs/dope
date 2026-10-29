use dope_core::io::transfer;
use dope_net::link::egress::data::{
    Offset,
    compose::{Either, Tagged},
    raw::Payload as _,
};
use o3::buffer::{raw::Scatter as _, segments, segments::scatter, storage};

#[test]
fn resume_keeps_owner() {
    let pool = segments::Pool::try_new(segments::Plan {
        segments: 3,
        chains: 1,
    })
    .expect("pool");
    let mut chain = pool.try_chain().expect("chain");
    for bytes in [b"ab".as_slice(), b"cde", b"fghi"] {
        let owner = storage::Shared::try_copy(bytes).expect("owner");
        assert!(chain.try_push(owner).is_ok());
    }
    let payload: Either<&[u8], _> = Either::Right(Tagged::new(chain, 7));
    let expected = b"abcdefghi";
    let mut position = scatter::raw::Position::new();
    let charge = payload.charge();
    let offset = Offset::from_len(transfer::Len::try_new(4).expect("offset"));
    // SAFETY: this unchanged owner retains its three segments throughout;
    // the requested offset is strictly inside its exact byte extent.
    let mut cursor = unsafe { payload.resume(&mut position, offset) };
    let mut used = 4;
    for chunk in cursor.take_bytes(expected.len()) {
        let bytes = chunk.bytes();
        assert_eq!(bytes, &expected[used..used + bytes.len()]);
        used += bytes.len();
    }
    assert_eq!(used, expected.len());
    assert_eq!(payload.charge(), charge);
}
