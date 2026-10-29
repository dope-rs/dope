pub mod common;

use common::codec::{self, input, try_with_budget};
use dope::manifold::client::session::codec::{Parse, Stateless, eof::Finish as _};
use dope_dns::Prefix;
use o3::buffer::storage;

#[test]
fn removes_length_prefix() {
    let codec = Prefix;
    let bytes = { (storage::Shared::try_copy(&codec::COMPLETE)).expect("complete frame storage") };
    try_with_budget(0, |budget| {
        let prepared = input(&codec, &bytes, &budget, |input| {
            let parsed = { (codec.parse(input)).expect("complete frame") };
            let (head, consumed) = {
                match parsed {
                    Parse::Item { head, consumed } => Some((head, consumed)),
                    _ => None,
                }
                .expect("complete owned frame must parse")
            };
            assert_eq!(head.as_slice(), &codec::PAYLOAD);
            assert_eq!(consumed.count().get() as usize, 5);
        });
        assert!(prepared.is_ok(), "frame staging plan");
    })
    .expect("resident budget");
}

#[test]
fn keeps_truncated_lengths() {
    let codec = Prefix;
    for (wire, expected) in codec::TRUNCATED {
        let bytes = { (storage::Shared::try_copy(wire)).expect("truncated frame storage") };
        try_with_budget(0, |budget| {
            let prepared = input(&codec, &bytes, &budget, |input| {
                let error = {
                    (codec.finish(input))
                        .err()
                        .expect("truncated frame must fail")
                };
                assert!(error == expected, "exact truncated failure");
            });
            assert!(prepared.is_ok(), "truncated staging plan");
        })
        .expect("resident budget");
    }
}
