#![deny(unsafe_code)]

use std::mem::size_of;

use dope_fiber::abi::{Join, Ready};

#[test]
fn reuses_child_slots() {
    let separate = 2 * size_of::<Ready<[u8; 256]>>() + 2 * size_of::<Option<[u8; 256]>>();
    assert!(size_of::<Join<'static, Ready<[u8; 256]>, Ready<[u8; 256]>>>() < separate);
}
