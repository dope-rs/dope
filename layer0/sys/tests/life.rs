pub mod common;

use std::cell::Cell;

use common::Batch;
use dope_sys::{engine::life::Reclaim as _, probe::Stopped};

#[test]
fn retains_every_fault() {
    let batch = Batch {
        entries: common::ENTRIES.into_iter(),
        reports: Vec::new(),
        reads: 0,
        calls: Cell::new(0),
    };
    let Stopped { mut state, cause } = batch.drain().expect_err("two completion faults");
    assert_eq!(cause, 7);
    assert_eq!(state.reports, [7, 11]);
    assert_eq!(state.reads, 4);
    assert_eq!(state.calls.get(), 0);
    assert!(state.entries.next().is_none());
    let state = state
        .drain()
        .expect("the returned owner has no unread entry");
    assert_eq!(state.reports, [7, 11]);
    assert_eq!(state.reads, 5);
    assert_eq!(state.calls.get(), 0);
}
