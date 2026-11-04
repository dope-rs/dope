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
    for manual in [false, true] {
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
                        let app = dispatch::Builder::new(Quiesce::default()).probe::<KeyTag<0>>();
                        let result = if manual {
                            let start = sess
                                .driver()
                                .driver_ref()
                                .clock()
                                .now()
                                .try_add(time::Duration::from_millis(1))
                                .expect("manual start deadline");
                            sess.with_at_for(&mut finish, app, start, timeout, |_| elapse(timeout))
                        } else {
                            sess.with_quiesce(&mut finish, app, timeout, |_| elapse(timeout))
                        };
                        assert!(
                            result.is_ok(),
                            "callback time must not consume the quiescence budget; manual={manual}"
                        );
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
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
}

fn elapse(timeout: time::Duration) {
    let start = time::Instant::now();
    while start.elapsed() <= timeout {
        thread::yield_now();
    }
}
