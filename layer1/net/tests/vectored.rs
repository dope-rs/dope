pub mod common;

use common::wire::contract::Case;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plan_keeps_owners() {
    let (bytes, queued, closing) = Case::Vectored.run();
    assert_eq!(bytes, b"abcdef");
    assert!(queued, "both owners remain queued until native completion");
    assert!(closing, "prepared close intent must reach the connection");
}

#[test]
fn local_consumption_returns_owners() {
    let (bytes, queued, closing) = Case::Consumed.run();
    assert!(bytes.is_empty());
    assert!(!queued);
    assert!(!closing);
}
