use dope::{
    core::driver::life::recovery,
    runtime::executor::session::{Completion, Error},
};
use dope_fiber::wait::Slot;
use dope_test::harness::scenario;

use crate::common::runtime::Empty;
pub mod common;

use core::array;
use std::{cell::Cell, convert::Infallible};

use dope_fiber::{
    BlockOn,
    task::batch::{Join, Pool},
    wait::Waiter,
};
use dope_test::alloc::Tracker;

use crate::common::{
    runtime::{self, Setup},
    schedule::batches::*,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
#[deny(unsafe_code)]
fn preserves_ready_round() {
    let mut finish = Completion::new();
    const WIDTH: usize = 512;
    let mut reports = Vec::new();

    Setup::throughput().with_session(
        |mut session| {
            let polls = [const { Cell::new(Some(0)) }; WIDTH];
            let ready = Cell::new(false);
            let wakes = Cell::new(Vec::with_capacity(WIDTH));
            let fibers: [FanIn<'_, '_, WIDTH>; WIDTH] = array::from_fn(|index| FanIn {
                index,
                polls: &polls[index],
                ready: &ready,
                wakes: &wakes,
            });
            let (outputs, (allocs, bytes)) = Tracker::usage(|| {
                session
                    .with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                        app.block_on(dope_fiber::fiber!('_ => async move {
                            let mut domain = Pool::<WIDTH>::acquire().await?;
                            Join::from_array(&mut domain, fibers)
                                .await
                        }))
                    })
                    .expect("batch session failed")
                    .expect("batch drive failed")
                    .expect("batch construction failed")
            });
            assert_eq!((allocs, bytes), (0, 0));
            let outputs = outputs.collect::<Vec<_>>();
            assert_eq!(outputs, (0..WIDTH).collect::<Vec<_>>());
            assert!(polls.iter().all(|polls| polls.get() == Some(2)));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
#[deny(unsafe_code)]
fn preserves_parked_child() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::throughput().with_session(
        |mut session| {
            let slot = Slot::new();
            let ready = Cell::new(false);
            let polls = [const { Cell::new(Some(0)) }; 2];
            let fibers = [
                ParkedChild {
                    follower: true,
                    slot: &slot,
                    ready: &ready,
                    polls: &polls[0],
                    waiter: Waiter::new(),
                },
                ParkedChild {
                    follower: false,
                    slot: &slot,
                    ready: &ready,
                    polls: &polls[1],
                    waiter: Waiter::new(),
                },
            ];

            let outputs = session.with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                app.block_on(dope_fiber::fiber!('_ => async move {
                    let mut domain = Pool::<2>::acquire().await?;
                    Join::from_array(&mut domain, fibers)
                        .await
                }))
            });
            assert!(
                outputs.is_ok(),
                "batch session failed: {:?}",
                outputs.as_ref().err().map(Error::cause)
            );
            let outputs = outputs
                .ok()
                .expect("batch session failed")
                .expect("batch drive failed")
                .expect("batch construction failed");
            let outputs = outputs
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.cause())
                .expect("follower owns the sole waiter slot");

            assert_eq!(outputs, [0, 1]);
            assert_eq!(polls[0].get(), Some(2));
            assert_eq!(polls[1].get(), Some(1));
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
