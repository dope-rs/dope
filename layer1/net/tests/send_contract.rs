pub mod common;

use common::wire::contract::Case;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn send_paths_no_alloc() {
    let (bytes, queued, closing) = Case::Buffered.run();
    assert_eq!(bytes, b"retained");
    assert!(queued, "buffered output must not consume queued input");
    assert!(closing, "prepared close intent must reach the connection");
}
