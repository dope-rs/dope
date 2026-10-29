pub mod common;

use common::runtime::topology::{Name, Number};
use dope_core::driver::storage;
use dope_runtime::topology::recipe;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn tuple_borrows() {
    let owner = String::from("driver-owned-service");
    let address = owner.as_ptr();
    let pair = (owner, 41_u64);

    let ((first, second), counts) = Tracker::usage(|| {
        let Ok(built) = <(Name, Number) as recipe::Build<(
            storage::Value<String>,
            storage::Value<u64>,
        )>>::build((Name, Number), &pair);
        built
    });
    assert_eq!(counts, (0, 0));
    assert_eq!(first.as_ptr(), address);
    assert_eq!(*second, 41);
}
