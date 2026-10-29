pub mod common;

use dope_manifold::client::session::codec::{
    Bytes, Input, Parse, Stateless,
    staging::{Buffered, Policy},
};
use o3::buffer::{resident::staging::Plan, storage};

use crate::common::{
    contract::codec::{Borrowed, Cursor, input},
    resident::try_with_budget,
};

#[test]
fn borrows_receive_chunk() {
    let bytes = { (storage::Shared::try_copy(b"borrowed")).expect("borrowed storage") };
    let expected = bytes.as_ptr();
    try_with_budget(0, |budget| {
        let prepared = input(&Borrowed, &bytes, &budget, |input| {
            let parsed = Borrowed.parse(input).expect("complete borrowed prefix");
            let (head, consumed) = {
                match parsed {
                    Parse::Item { head, consumed } => Some((head, consumed)),
                    _ => None,
                }
                .expect("non-empty input must yield an item")
            };
            assert_eq!(head.as_ptr(), expected);
            assert_eq!(head, b"borrowed");
            assert_eq!(consumed.count().get() as usize, bytes.len());
        });
        assert!(prepared.is_ok(), "borrowed staging plan");
    })
    .expect("resident budget");
}

#[test]
fn plans_deferred_range() {
    let bytes = { (storage::Shared::try_copy(b"xxpayloadxx")).expect("source storage") };
    let source = bytes.as_ptr().wrapping_add(2);
    try_with_budget(7, |budget| {
        let plan = { (Plan::new(1, 1)).expect("direct test plan") };
        let limit = <Buffered as Policy>::limit(plan);
        let deferred = {
            (Input::with(&bytes, &budget, limit, |input| input.try_plan(2..9)))
                .expect("the codec range must be in bounds")
        };
        assert_eq!(deferred.as_slice().as_ptr(), source);
        assert_eq!(deferred.as_slice(), b"payload");
    })
    .expect("resident budget");
}

#[test]
fn defers_receive_chunk() {
    let bytes = Cursor(*b"bytes");
    let expected = bytes.0.as_ptr();
    try_with_budget(0, |budget| {
        let prepared = input(&Bytes, &bytes, &budget, |input| {
            let parsed = { (Bytes.parse(input)).expect("deferred byte range") };
            let (head, consumed) = {
                match parsed {
                    Parse::Item { head, consumed } => Some((head, consumed)),
                    _ => None,
                }
                .expect("non-empty input must yield its deferred chunk")
            };
            assert_eq!(head.as_slice().as_ptr(), expected);
            assert_eq!(head.as_slice(), b"bytes");
            assert_eq!(consumed.count().get() as usize, bytes.0.len());
        });
        assert!(prepared.is_ok(), "byte staging plan");
    })
    .expect("resident budget");
}
