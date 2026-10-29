use std::convert::Infallible;

use dope_core::driver::life::{creation::Attempt, recovery};
pub mod common;

use std::{cell::Cell, ptr, rc::Rc};

use dope_core::driver::{life::domain, route::lease, storage};
use dope_manifold::app::creation;
use o3::depot::capacity;

use crate::common::{runtime::executor::storage::*, scenario::rt::Configured};

#[test]
fn plan_failure() {
    let mut create = Attempt::new();
    let mut reports = Vec::new();
    let executor = Configured::throughput().try_executor(&mut create);
    assert!(executor.is_ok(), "executor construction");
    let Some(executor) = executor.ok() else {
        return;
    };
    let borrowed = Cell::new(7);
    let local = Rc::new(());
    let identity = Rc::as_ptr(&local);
    let plan = Collision {
        capacity: capacity::Count::new(1),
        borrowed: &borrowed,
        local,
    };
    executor.with_plan(plan).run(|executor| {
        let result = executor.try_create(
            |session| {
                let (first, second) = session.storage();
                (first.has_room(), second.has_room())
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(matches!(
            &result,
            Err(domain::Error::Storage(domain::Rejected {
                cause: Failure {
                    cause: creation::Error::Route(lease::Error::Occupied),
                    borrowed: actual,
                    local,
                },
                ..
            })) if ptr::eq(*actual, &borrowed)
                && actual.get() == 7
                && Rc::as_ptr(local) == identity
                && Rc::strong_count(local) == 1
        ));

        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    });
}

#[test]
fn composed_plan() {
    let mut create = Attempt::new();
    let executor = Configured::throughput().try_executor(&mut create);
    assert!(executor.is_ok(), "executor construction");
    let Some(executor) = executor.ok() else {
        return;
    };
    let dropped = [Cell::new(None), Cell::new(None)];
    let plan = (
        storage::Value::new(Pinned::new(&dropped[0])),
        storage::Value::new(Pinned::new(&dropped[1])),
    );
    let mut reports = Vec::new();

    executor.with_plan(plan).run(|executor| {
        let result = executor.enter_with(
            |session| {
                let pair = session.storage();
                pair.0.record();
                pair.1.record();
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "pinned pair generation and finalization");
    });
    assert_eq!(dropped.map(Cell::into_inner), [Some(true); 2]);
}

#[test]
fn pinned_drop() {
    let mut create = Attempt::new();
    let executor = Configured::throughput().try_executor(&mut create);
    assert!(executor.is_ok(), "executor construction");
    let Some(executor) = executor.ok() else {
        return;
    };
    let mut reports = Vec::new();

    let dropped = Cell::new(None);
    executor
        .with_storage(Pinned::new(&dropped))
        .run(|executor| {
            let result = executor.enter_with(
                |session| session.storage().record(),
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            assert!(
                reports.is_empty(),
                "unexpected reclaimed operations: {reports:?}"
            );

            assert!(result.is_ok(), "pinned owner generation and finalization");
        });
    assert_eq!(dropped.get(), Some(true));
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
