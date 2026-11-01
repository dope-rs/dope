pub mod common;

use std::{os::unix::process::ExitStatusExt as _, thread, time, time::Duration};

use dope_core::driver::life::{creation::Attempt, recovery};
use dope_manifold::timing::watchdog;
use dope_runtime::executor::session::Completion;
use dope_test::{fibers, harness::scenario::process::Test};
use o3::base::time::Positive;

use crate::common::{manifold::Host, recover, scenario::Runtime};

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
fn stall_is_killed() {
    let output = Test {
        name: "stall_is_killed",
    }
    .try_signal(|| {
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
                    let watchdog =
                        watchdog::Watchdog::new(config, &mut session.driver()).expect("watchdog");
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
    })
    .expect("watchdog child");
    if let Some(output) = output {
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
    }
}

#[test]
fn period_before_deadline() {
    let period = Positive::from_millis::<100>();
    assert!(watchdog::Config::new(period, period).is_err());
}
