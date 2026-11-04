pub mod common;

use common::runtime::topology::Name;
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
        let Ok(built) = <(Name, recipe::Borrow) as recipe::Build<(
            storage::Value<String>,
            storage::Value<u64>,
        )>>::build((Name, recipe::Borrow), &pair);
        built
    });
    assert_eq!(counts, (0, 0));
    assert_eq!(first.as_ptr(), address);
    assert!(std::ptr::eq(second, &pair));
    assert_eq!(second.1, 41);
}
