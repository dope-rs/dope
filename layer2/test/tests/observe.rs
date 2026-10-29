use std::{cell::Cell, rc::Rc};

use dope_test::harness::observe::{Error, Trace};

pub mod common;

#[test]
fn retains_rejected_owner() {
    let dropped = Cell::new(Vec::new());
    let trace = Rc::new(Trace::new());
    for value in [1, 2] {
        trace.record(common::Recorded {
            value,
            trace: Rc::downgrade(&trace),
            dropped: &dropped,
        });
    }
    assert_eq!(dropped.take(), [(2, Some(Error::Full))]);
    assert_eq!(trace.values().err(), Some(Error::Full));
    drop(trace);
    assert_eq!(dropped.take(), [(1, None)]);
}

#[test]
fn keeps_borrow_failure() {
    let trace = Trace::<_, 2>::new();
    let values = trace.values().expect("empty trace");
    trace.record(7);
    drop(values);
    trace.push(9).expect("released observation borrow");
    assert_eq!(trace.values().err(), Some(Error::Borrowed));
}
