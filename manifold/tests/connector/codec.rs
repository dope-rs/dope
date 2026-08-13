use std::{convert::Infallible, num::NonZeroUsize};

use dope_manifold::connector::codec::{Codec, Error, Input, LengthPrefixed, Parse};
use o3::{
    buffer::{resident, storage},
    cell::region,
};

struct CapacityCursor([u8; 5]);

struct BorrowedCodec;

impl Codec for BorrowedCodec {
    type Head<'input, 'd> = &'input [u8];
    type ParseState = ();
    type Error = Infallible;

    fn parse_state(&self) {}

    fn parse<'input, 'd, R: dope_net::wire::Cursor<'d>>(
        &self,
        _state: &mut Self::ParseState,
        input: Input<'input, 'd, R>,
    ) -> Result<Parse<Self::Head<'input, 'd>>, Self::Error>
    where
        'd: 'input,
    {
        let bytes = input.as_slice();
        let Some(consumed) = NonZeroUsize::new(bytes.len()) else {
            return Ok(Parse::NeedMore);
        };
        Ok(Parse::Item {
            head: bytes,
            consumed,
        })
    }

    fn finish<'d>(
        &self,
        _state: &mut Self::ParseState,
        _remaining: dope_net::wire::RetainedBytes<'d>,
    ) -> Result<Option<Self::Head<'d, 'd>>, Self::Error> {
        Ok(None)
    }
}

impl<'d> dope_net::wire::Cursor<'d> for CapacityCursor {
    fn chunk(&self) -> &[u8] {
        &self.0
    }

    fn consume(&mut self, requested: usize) -> usize {
        requested.min(self.0.len())
    }

    fn remaining(&self) -> usize {
        self.0.len()
    }

    fn retain(
        &self,
        range: std::ops::Range<usize>,
        _: &resident::Budget<'d>,
    ) -> Result<dope_net::wire::RetainedBytes<'d>, dope_net::wire::RetainError> {
        if self.0.get(range).is_none() {
            return Err(dope_net::wire::RetainError::Range);
        }
        Err(dope_net::wire::RetainError::Capacity)
    }
}

#[test]
fn length_prefix_is_removed_from_the_frame() {
    let codec = LengthPrefixed::<4>;
    let bytes = storage::Shared::copy_from_slice(&[0, 3, 7, 8, 9]);
    region::Token::scope(|token| {
        let budget = resident::Budget::new(0, &token);
        match codec
            .parse(&mut (), Input::new(&bytes, &budget))
            .expect("frame")
        {
            Parse::Item { head, consumed } => {
                assert_eq!(head.as_ref(), &[7, 8, 9]);
                assert_eq!(consumed.get(), 5);
            }
            Parse::NeedMore => panic!("complete frame must parse"),
            Parse::CapacityExhausted => panic!("owned input does not consume resident capacity"),
        }
    });
}

#[test]
fn length_ceiling_is_enforced_before_payload_arrives() {
    let codec = LengthPrefixed::<2>;
    let bytes = storage::Shared::copy_from_slice(&[0, 3]);
    region::Token::scope(|token| {
        let budget = resident::Budget::new(0, &token);
        match codec.parse(&mut (), Input::new(&bytes, &budget)) {
            Err(error) => assert_eq!(
                error,
                Error::Capacity {
                    length: 3,
                    limit: 2,
                }
            ),
            Ok(_) => panic!("oversized prefix must fail"),
        }
    });
}

#[test]
fn retained_capacity_is_distinct_from_incomplete_input() {
    let codec = LengthPrefixed::<4>;
    let bytes = CapacityCursor([0, 3, 7, 8, 9]);
    region::Token::scope(|token| {
        let budget = resident::Budget::new(0, &token);
        assert!(matches!(
            codec.parse(&mut (), Input::new(&bytes, &budget)),
            Ok(Parse::CapacityExhausted)
        ));
    });
}

#[test]
fn codec_can_borrow_the_receive_chunk_without_retaining_it() {
    let bytes = storage::Shared::copy_from_slice(b"borrowed");
    let expected = bytes.as_ptr();
    region::Token::scope(|token| {
        let budget = resident::Budget::new(0, &token);
        let parsed = BorrowedCodec
            .parse(&mut (), Input::new(&bytes, &budget))
            .expect("infallible borrowed parse");
        let Parse::Item { head, consumed } = parsed else {
            panic!("non-empty input must yield an item");
        };
        assert_eq!(head.as_ptr(), expected);
        assert_eq!(head, b"borrowed");
        assert_eq!(consumed.get(), bytes.len());
    });
}
