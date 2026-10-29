use std::{cell::Cell, mem, num::NonZero};

use dope_core::{
    dope_sys::io::socket::record::Record,
    platform::{
        channel::Channel,
        thread::{Builder, Workers},
    },
};

pub mod common;
use common::rt::thread::{Gated, Order, Owned, STACK, Write};

#[test]
fn mutates_live_parent() {
    let mut target = 0;
    let (gate, channel) = Channel::pair().expect("worker gate");
    let mut builder = Builder::try_new(1).expect("worker storage");
    builder
        .try_push(
            Gated {
                channel,
                task: Write {
                    target: &mut target,
                    value: 7,
                },
            },
            10,
        )
        .expect("gated task");
    let (before, ready, count, release, joined) = builder.scope(|mut workers| {
        let before = workers.parents_mut().len();
        workers.start(STACK).expect("start gated worker");
        let ready = gate.recv();
        let count = {
            let mut parents = workers.parents_mut();
            let count = parents.len();
            if let Some(parent) = parents.next() {
                *parent = 11;
            }
            count
        };
        let release = gate.send(2);
        drop(gate);
        let joined = workers.finish().next();
        (before, ready, count, release, joined)
    });
    assert_eq!((before, count), (0, 1));
    assert!(
        matches!(ready, Record::Value(1)),
        "worker readiness: {ready:?}"
    );
    release.expect("release gated worker");
    let (parent, output) = joined.expect("joined worker");
    assert_eq!((parent, output.expect("worker handshake")), (11, 7));
    assert_eq!(target, 7);
}

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
        workers.start(STACK).expect("start workers");
        workers.start(STACK).expect("already started");
        let mut finished = workers.finish();
        (finished.next(), finished.next(), finished.next())
    });
    assert_eq!(result, (Some((10, 7)), Some((20, 9)), None));
    assert_eq!((first, second), (7, 9));
}

#[test]
fn builder_drops_input() {
    let mut drops = 0;
    let mut builder = Builder::<_, Owned<'_>, _>::try_new(1).expect("worker storage");
    builder.try_push(Owned(&mut drops), ()).expect("task");
    drop(builder);
    assert_eq!(drops, 1);
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
    const { assert!(!mem::needs_drop::<Workers<'_, Write<'_>, u64, ()>>()) };
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
fn scope_drops_output() {
    let mut drops = 0;
    let mut builder = Builder::try_new(1).expect("worker storage");
    builder.try_push(Owned(&mut drops), ()).expect("task");
    builder.scope(|mut workers| workers.start(STACK).expect("start worker"));
    assert_eq!(drops, 1);
}

#[test]
fn partial_drops_remainder() {
    let mut first = 0;
    let mut second = 0;
    let mut builder = Builder::try_new(2).expect("worker storage");
    builder.try_push(Owned(&mut first), ()).expect("first task");
    builder
        .try_push(Owned(&mut second), ())
        .expect("second task");
    builder.scope(|mut workers| {
        workers.start(STACK).expect("start workers");
        let mut finished = workers.finish();
        finished.next().expect("first output")
    });
    assert_eq!((first, second), (1, 1));
}
