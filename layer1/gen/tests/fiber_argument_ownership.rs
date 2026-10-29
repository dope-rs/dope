pub mod common;

use std::cell::Cell;

use common::owners::{self, Guard, Order};

#[test]
fn retains_arguments() {
    let drops = Cell::new(0);
    let named = owners::named(Guard(&drops));
    let pair = owners::destructured((Guard(&drops), Guard(&drops)));
    let receiver = Guard(&drops).receiver();
    assert_eq!(drops.get(), 0);
    drop((named, pair, receiver));
    assert_eq!(drops.get(), 4);
}

#[test]
fn unpolled_order() {
    let order = Cell::new(0);
    let values = || (1, (Order(&order, 1), Order(&order, 2)), Order(&order, 3));
    let (value, pair, last) = values();
    drop(owners::original(value, pair, last));
    let expected = order.replace(0);
    let (value, pair, last) = values();
    drop(owners::patterns(value, pair, last));
    assert_eq!(order.get(), expected);
}
