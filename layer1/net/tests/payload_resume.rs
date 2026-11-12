use dope_core::io::transfer;
use dope_net::link::egress::data::{
    Offset,
    compose::{Either, Tagged},
    position,
    raw::Payload as _,
};
use o3::buffer::{raw::Scatter as _, segments, storage};

#[test]
fn resume_keeps_owner() {
    let pool = segments::Pool::try_new(segments::Plan {
        segments: 3,
        chains: 1,
    })
    .expect("pool");
    for left in [true, false] {
        let mut chain = pool.try_chain().expect("chain");
        for bytes in [b"ab".as_slice(), b"cde", b"fghi"] {
            let owner = storage::Shared::try_copy(bytes).expect("owner");
            assert!(chain.try_push(owner).is_ok());
        }
        let payload: Either<_, Either<&[u8], _>> = if left {
            Either::Left(chain)
        } else {
            Either::Right(Either::Right(Tagged::new(chain, 7)))
        };
        let expected = b"abcdefghi";
        let mut position = position::Saved::default();
        let charge = payload.charge();
        for start in [4, 7] {
            let offset = Offset::from_len(transfer::Len::try_new(start).expect("offset"));
            // SAFETY: each offset lies within the same retained payload.
            let mut cursor = unsafe { payload.resume(&mut position, offset) };
            let mut used = start;
            for chunk in cursor.take_bytes(expected.len()) {
                let bytes = chunk.bytes();
                assert_eq!(bytes, &expected[used..used + bytes.len()]);
                used += bytes.len();
            }
            assert_eq!(used, expected.len());
        }
        assert_eq!(payload.charge(), charge);
    }
}
