use std::{convert::Infallible, time};

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    settings,
};
use dope_runtime::{executor, executor::session::Completion, shutdown};
use dope_test::harness::{observe::Trace, scenario::TIMEOUT, server::Pair};

pub const STARTUP_TIMEOUT: time::Duration = time::Duration::from_millis(20);
pub const E2E_DEADLINE: time::Duration = time::Duration::from_secs(2);
pub const LATE_STARTUP: time::Duration = time::Duration::from_millis(50);

#[derive(Debug)]
pub struct LateFailure;

pub fn with_harness(deadline: time::Duration, run: impl FnOnce(Pair)) {
    let harness = Pair::bind().and_then(|harness| harness.timeouts(STARTUP_TIMEOUT, deadline));
    let harness = (harness).expect("bounded harness");
    run(harness);
}

pub fn serve(source: shutdown::ReadySource) {
    let mut finish = Completion::new();
    let config = settings::Config::new(
        settings::QueueLayout::fixed::<256, 1024>(),
        settings::Schedule::DEFAULT,
        settings::FileSlots::fixed::<0, 16>(),
        settings::Datagram::fixed::<2, 8>().receive(),
        settings::Domain::ONE,
        settings::Progress::Prompt,
    );
    let mut create = Attempt::new();
    let executor = executor::Build::try_build(source, &mut create, config);
    assert!(executor.is_ok(), "executor");
    let Some(executor) = executor.ok() else {
        return;
    };
    let mut reports = Vec::new();

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let trace = Trace::<_, 0>::new();
                session
                    .run_quiesce(&mut finish, &trace, TIMEOUT)
                    .expect("application teardown")
                    .expect("application drive");
                assert!(trace.values().expect("application observations").is_empty());
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "server executor must finish");
    });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
