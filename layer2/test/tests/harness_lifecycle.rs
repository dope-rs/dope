use std::{
    cell::Cell, convert::Infallible, env, os::unix::process::ExitStatusExt as _, thread::sleep,
    time,
};

use dope_core::platform::{thread, wake};
use dope_runtime::{process::STACK, shutdown};
use dope_test::harness::{
    scenario::{process::Test, run},
    server::Pair,
};

pub mod common;
use common::harness::{
    E2E_DEADLINE, LATE_STARTUP, LateFailure, STARTUP_TIMEOUT, serve, with_harness,
};

fn assert_startup_timeout<E>(
    error: run::Error<E, wake::CreateError, shutdown::FireError, wake::WaitError>,
) {
    let (timeout, elapsed) = match error {
        run::Error::Readiness(wake::WaitError::Timeout { timeout, elapsed }) => {
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
                sleep(LATE_STARTUP);
                serve(source);
                Ok::<_, Infallible>(())
            },
            |_| started.set(true),
        );
        assert!(
            error.is_err(),
            "late startup must report its readiness deadline"
        );
        if let Err(error) = error {
            assert_startup_timeout(error);
        }
        assert!(!started.get(), "late startup must not invoke the client");
    });
}

#[test]
fn timeout_beats_failure() {
    with_harness(E2E_DEADLINE, |harness| {
        let started = Cell::new(false);
        let error = harness.verify(
            |source| {
                sleep(LATE_STARTUP);
                Err::<(), _>((LateFailure, source))
            },
            |_| started.set(true),
            |result| assert!(result.is_err(), "late server failure"),
        );
        assert!(
            error.is_err(),
            "late server failure must retain the startup deadline"
        );
        if let Err(error) = error {
            assert_startup_timeout(error);
        }
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
fn child_succeeds() {
    let output = Test {
        name: "child_succeeds",
    }
    .try_run(|| {})
    .expect("isolated success");
    if let Some(output) = output {
        assert!(output.status.success(), "{output:?}");
    }
}

#[test]
fn child_fails() {
    let output = Test {
        name: "child_fails",
    }
    .try_run(|| panic!("isolated failure"))
    .expect("isolated failure");
    if let Some(output) = output {
        assert!(!output.status.success(), "{output:?}");
    }
}

#[test]
fn watchdog_kills_hang() {
    let output = Test {
        name: "watchdog_kills_hang",
    }
    .try_signal(|| {
        with_harness(time::Duration::from_millis(100), |harness| {
            harness
                .run_unbound(
                    |source| {
                        let (wait, notify) =
                            wake::Ends::blocking().expect("stalled server gate").split();
                        wait.wait().expect("stalled server gate remains connected");
                        drop((notify, source));
                        Ok::<_, Infallible>(())
                    },
                    |addr| addr,
                )
                .expect("stalled server returned before the watchdog killed it");
        });
    })
    .expect("spawn watchdog child test");
    if let Some(output) = output {
        assert_eq!(output.status.signal(), Some(libc::SIGKILL));
    }
}

#[test]
fn watchdog_releases_locks() {
    use std::{fs, io, os::fd::AsRawFd as _};

    use dope_test::harness::support::file::Directory;

    // Other tests may spawn children that briefly inherit descriptors before exec.
    let output = Test {
        name: "watchdog_releases_locks",
    }
    .try_run(|| {
        let base = env::temp_dir();
        let dir = Directory::try_with(&base, "watchdog-lock", |closed| {
            closed.expect("temporary directory cleanup");
        })
        .expect("temporary path");
        let path = dir.path().join("owner");
        let owner = fs::File::create_new(&path).expect("lock file");
        let harness = Pair::bind().expect("watchdog harness");
        let result = harness.run_unbound(
            |source| {
                {
                    let owner = owner;
                    let locked =
                        unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                    assert_eq!(locked, 0, "initial lock: {}", io::Error::last_os_error());
                }
                let next = fs::File::open(&path).expect("reopen lock file");
                let locked =
                    unsafe { libc::flock(next.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                assert_eq!(
                    locked,
                    0,
                    "watchdog retained lock: {}",
                    io::Error::last_os_error()
                );
                serve(source);
                Ok::<_, Infallible>(())
            },
            |_| (),
        );
        assert!(result.is_ok(), "watchdog harness must finish");
    })
    .expect("isolated descriptor ownership");
    if let Some(output) = output {
        assert!(output.status.success(), "{output:?}");
    }
}
