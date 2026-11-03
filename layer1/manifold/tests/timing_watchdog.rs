pub mod common;

use std::{cell::Cell, os::unix::process::ExitStatusExt as _, thread, time, time::Duration};

use dope_core::driver::life::{creation::Attempt, recovery};
use dope_manifold::timing::watchdog;
use dope_runtime::executor::session::Completion;
use dope_test::{fibers, harness::scenario::process::Test};
use o3::base::time::Positive;

use crate::common::{
    manifold::{Host, root::Root},
    recover,
    scenario::Runtime,
};

#[test]
fn progress_disarms() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let executor = {
        let result = Runtime::throughput().try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    };
    let mut reports = Vec::new();

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let config = (Host::config::<20, 200>()).expect("watchdog config");
                let watchdog =
                    (watchdog::Watchdog::new(config, &mut session.driver())).expect("watchdog");
                let result = session.with_quiesce(
                    &mut finish,
                    Host { watchdog },
                    Duration::from_secs(2),
                    |mut app| {
                        fibers::TEST
                            .try_pause(&mut app, time::Duration::from_millis(500))
                            .expect("watchdog progress");
                    },
                );
                assert!(result.is_ok(), "application teardown");
                thread::sleep(time::Duration::from_millis(250));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(result.is_ok(), "executor teardown");
    });
}

#[test]
fn saturated_manual_time_quiesces() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let executor = {
        let result = Runtime::throughput().try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    };
    let mut reports = Vec::new();

    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let start = session.driver().driver_ref().clock().now();
                let last = start.clamp_add(Duration::MAX);
                let config = Host::config::<20, 200>().expect("watchdog config");
                let watchdog =
                    watchdog::Watchdog::new(config, &mut session.driver()).expect("watchdog");
                let result = session.with_at_for(
                    &mut finish,
                    Host { watchdog },
                    start.into(),
                    Duration::from_secs(2),
                    |mut app| {
                        let polls = Cell::new(0);
                        app.drive_for(last, Duration::from_millis(100), Root(&polls))
                            .map(|fixed| fixed.into_output())
                    },
                );
                assert!(result.is_ok(), "application teardown");
                let driven = result.ok().expect("application teardown");
                assert!(
                    driven.is_ok(),
                    "saturated watchdog must reach a fixed point"
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "executor teardown");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn stall_is_killed() {
    let output = Test {
        name: "stall_is_killed",
    }
    .signal(
        &std::env::current_exe().expect("test executable"),
        std::env::var_os(dope_test::harness::scenario::process::CHILD).as_deref(),
        || {
            let mut create = Attempt::new();
            let executor = {
                let result = Runtime::throughput().try_executor(&mut create);
                result
                    .as_ref()
                    .map_err(|error| error.kind())
                    .expect("native creation");
                result.ok().expect("native creation")
            };
            let mut finish = Completion::new();
            executor.run(|executor| {
                let result = executor.enter_with(
                    |mut session| {
                        let config = Host::config::<20, 150>().expect("watchdog config");
                        let watchdog = watchdog::Watchdog::new(config, &mut session.driver())
                            .expect("watchdog");
                        let result = session.with_quiesce(
                            &mut finish,
                            Host { watchdog },
                            Duration::from_secs(2),
                            |_app| thread::sleep(Duration::from_secs(2)),
                        );
                        assert!(result.is_ok(), "application teardown");
                    },
                    recovery::Policy::new(drop, recover),
                );
                assert!(result.is_ok(), "executor teardown");
            });
        },
    )
    .map(|mut child| child.output().expect("watchdog child"));
    if let Some(output) = output {
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
    }
}

#[test]
fn period_before_deadline() {
    let period = Positive::from_millis::<100>();
    assert!(watchdog::Config::new(period, period).is_err());
}
