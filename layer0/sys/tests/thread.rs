pub mod common;

use std::{cell::Cell, num::NonZero, ptr::NonNull};

use common::thread::Completed;
use dope_sys::thread::{Attrs, STACK_MIN, raw::Entry};

#[test]
fn destruction_joins() {
    let attrs = Attrs::with_stack(NonZero::new(STACK_MIN).expect("native stack minimum"))
        .expect("thread attributes");
    let mut completed = Completed(Cell::new(false));
    // SAFETY: completed stays at this address and only the worker accesses its
    // cell until the temporary handle is destroyed and joins below.
    unsafe { Entry::new(NonNull::from(&mut completed)) }
        .spawn(&attrs)
        .expect("native worker");
    assert!(completed.0.get());
}
