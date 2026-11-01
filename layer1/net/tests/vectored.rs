pub mod common;

use common::wire::contract::Case;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plan_keeps_owners() {
    let (bytes, queued, closing) = Case::Vectored.run();
    assert_eq!(bytes, b"abcdef");
    assert!(!queued, "exact admitted progress must consume both owners");
    assert!(closing, "prepared close intent must reach the connection");
}
