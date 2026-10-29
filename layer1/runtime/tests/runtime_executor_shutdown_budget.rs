use std::convert::Infallible;

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::KeyTag,
};
use dope_runtime::executor::session::Completion;
pub mod common;

use std::{thread, time};

use crate::common::{
    runtime::executor::shutdown::budget::*,
    scenario::{dispatch, rt::Configured},
};

#[test]
fn starts_after_call() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let timeout = time::Duration::from_millis(50);
                    assert!(
                        sess.with_quiesce(
                            &mut finish,
                            dispatch::Builder::new(Quiesce::default()).probe::<KeyTag<0>>(),
                            timeout,
                            |_| elapse(timeout)
                        )
                        .is_ok(),
                        "callback time must not consume the quiescence budget"
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn manual_starts_after_call() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let start = (sess
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(time::Duration::from_millis(1)))
                    .expect("manual start deadline");
                    let timeout = time::Duration::from_millis(50);
                    assert!(
                        sess.with_at_for(
                            &mut finish,
                            dispatch::Builder::new(Quiesce::default()).probe::<KeyTag<0>>(),
                            start,
                            timeout,
                            |_| elapse(timeout)
                        )
                        .is_ok(),
                        "callback time must not consume the manual quiescence budget"
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn elapse(timeout: time::Duration) {
    let start = time::Instant::now();
    while start.elapsed() <= timeout {
        thread::yield_now();
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
