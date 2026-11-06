use std::{
    cell::Cell, convert::Infallible, os::unix::process::ExitStatusExt as _, thread::sleep, time,
};

use dope_core::platform::thread;
use dope_runtime::process::{STACK, lifecycle};
use dope_test::harness::{
    scenario::{process::Test, run},
    server::Pair,
};

pub mod common;
use common::harness::{E2E_DEADLINE, LateFailure, STARTUP_TIMEOUT, serve, with_harness};

fn assert_startup_timeout<E>(error: run::Error<E>) {
    let (timeout, elapsed) = match error {
        run::Error::Readiness(lifecycle::WaitError::Timeout { timeout, elapsed }) => {
            Some((timeout, elapsed))
        }
        _ => None,
    }
    .expect("startup deadline must retain its exact timeout failure");
    assert_eq!(timeout, STARTUP_TIMEOUT);
    assert!(elapsed >= timeout);
}

#[test]
fn late_install_times_out() {
    with_harness(E2E_DEADLINE, |harness| {
        let started = Cell::new(false);
        let error = harness.run_unbound(
            |source| {
                source
                    .control()
                    .wait_stop_until(time::Instant::now() + E2E_DEADLINE)
                    .expect("startup timeout requests shutdown");
                serve(source);
                Ok::<_, Infallible>(())
            },
            |_| started.set(true),
        );
        assert_startup_timeout(error.expect_err("late startup must report its readiness deadline"));
        assert!(!started.get(), "late startup must not invoke the client");
    });
}

#[test]
fn timeout_beats_failure() {
    with_harness(E2E_DEADLINE, |harness| {
        let started = Cell::new(false);
        let error = harness.run_unbound(
            |source| {
                source
                    .control()
                    .wait_stop_until(time::Instant::now() + E2E_DEADLINE)
                    .expect("startup timeout requests shutdown");
                Err::<(), _>(LateFailure)
            },
            |_| started.set(true),
        );
        assert_startup_timeout(
            error.expect_err("late server failure must retain the startup deadline"),
        );
        assert!(!started.get(), "failed startup must not invoke the client");
    });
}

#[test]
fn watchdogs_are_isolated() {
    let mut group = thread::Builder::try_new(8).expect("parallel watchdog owners");
    for _ in 0..8 {
        group
            .try_push(
                run::Entry(|| {
                    let harness = Pair::bind().expect("parallel harness");
                    harness.run_unbound(
                        |source| {
                            serve(source);
                            Ok::<_, Infallible>(())
                        },
                        |addr| addr.ip().is_loopback(),
                    )
                }),
                (),
            )
            .expect("parallel watchdog input");
    }
    group.scope(|mut workers| {
        workers.start(STACK).expect("parallel watchdog start");
        for ((), result) in workers.finish() {
            assert!(matches!(result, Ok(true)), "independent watchdog");
        }
    });
}

#[test]
fn watchdog_kills_hang() {
    let output = Test {
        name: "watchdog_kills_hang",
    }
    .signal(
        &std::env::current_exe().expect("current test executable"),
        std::env::var_os(dope_test::harness::scenario::process::CHILD).as_deref(),
        || {
            with_harness(time::Duration::from_millis(100), |harness| {
                harness
                    .run_unbound(
                        |_source| {
                            sleep(E2E_DEADLINE);
                            Ok::<_, Infallible>(())
                        },
                        |addr| addr,
                    )
                    .expect("stalled server returned before the watchdog killed it");
            });
        },
    )
    .map(|mut child| child.output().expect("spawn watchdog child test"));
    if let Some(output) = output {
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{output:?}");
    }
}
