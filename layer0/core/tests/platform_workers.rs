use std::{cell::Cell, num::NonZero};

use dope_core::platform::thread::Builder;

pub mod common;
use common::rt::thread::{Order, Owned, STACK, Write};

#[test]
fn joins_ordered_outputs() {
    let mut first = 0;
    let mut second = 0;
    let mut builder = Builder::try_new(2).expect("worker storage");
    builder
        .try_push(
            Write {
                target: &mut first,
                value: 7,
            },
            10,
        )
        .expect("first task");
    builder
        .try_push(
            Write {
                target: &mut second,
                value: 9,
            },
            20,
        )
        .expect("second task");
    let result = builder.scope(|mut workers| {
        assert_eq!(workers.parents_mut().len(), 0);
        workers.start(STACK).expect("start workers");
        workers.start(STACK).expect("already started");
        assert_eq!(workers.parents_mut().len(), 2);
        for parent in workers.parents_mut() {
            *parent += 1;
        }
        let mut finished = workers.finish();
        (finished.next(), finished.next(), finished.next())
    });
    assert_eq!(result, (Some((11, 7)), Some((21, 9)), None));
    assert_eq!((first, second), (7, 9));
}

#[test]
fn drops_unstarted_in_order() {
    let order = Cell::new(0);
    let mut builder = Builder::<_, (), _>::try_new(2).expect("worker storage");
    for (task, parent) in [(1, 2), (3, 4)] {
        builder
            .try_push(
                Order {
                    order: &order,
                    value: task,
                },
                Order {
                    order: &order,
                    value: parent,
                },
            )
            .expect("task and parent");
    }
    drop(builder);
    assert_eq!(order.get(), 1234);
}

#[test]
fn failed_start_drops_input() {
    let mut drops = 0;
    let mut builder = Builder::try_new(1).expect("worker storage");
    builder.try_push(Owned(&mut drops), 13).expect("task");
    builder.scope(|mut workers| {
        let (parent, _) = workers
            .start(NonZero::<usize>::MIN)
            .expect_err("invalid stack");
        assert_eq!(*parent, 13);
    });
    assert_eq!(drops, 1);
}

#[test]
fn early_return_joins() {
    let mut result = 0;
    let mut builder = Builder::try_new(1).expect("worker storage");
    builder
        .try_push(
            Write {
                target: &mut result,
                value: 17,
            },
            (),
        )
        .expect("task");
    let returned = builder.scope(|mut workers| {
        workers.start(STACK).expect("start worker");
        Err::<(), _>("controller")
    });
    assert_eq!(returned, Err("controller"));
    assert_eq!(result, 17);
}

#[test]
fn drops_uncollected_outputs() {
    for take_first in [false, true] {
        let mut first = 0;
        let mut second = 0;
        let mut builder = Builder::try_new(2).expect("worker storage");
        builder.try_push(Owned(&mut first), ()).expect("first task");
        builder
            .try_push(Owned(&mut second), ())
            .expect("second task");
        let output = builder.scope(|mut workers| {
            workers.start(STACK).expect("start workers");
            if take_first {
                Some(workers.finish().next().expect("first output"))
            } else {
                None
            }
        });
        drop(output);
        assert_eq!((first, second), (1, 1), "yield first: {take_first}");
    }
}
