use std::pin::pin;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dope_core::backend::{Backend, Sqe};
use dope_core::driver;
use dope_core::driver::Driver;
use dope_core::driver::completion::Completion;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ext::DriverExt;
use dope_core::driver::submission::Submission;
use dope_core::driver::token::{Epoch, SlotIndex, Token, kind};
use dope_core::platform::Platform;
use dope_core::platform::snapshot::{Mismatch, Snapshot};
use dope_test::{throughput_cfg, with_driver};

const ROUTE: u8 = 7;

fn target() -> Token {
    Token::new(ROUTE, SlotIndex::new(0), Epoch::INITIAL)
}

type TimerSpec = <Backend as Platform>::TimerSpec;

fn spec() -> &'static TimerSpec {
    static SPEC: OnceLock<TimerSpec> = OnceLock::new();
    SPEC.get_or_init(|| TimerSpec::from(Duration::from_millis(10)))
}

#[test]
fn interval_emits_repeated_completions_and_can_be_cancelled() {
    with_driver(|mut driver| {
        driver
            .push(Sqe::interval(spec(), target()))
            .expect("arm interval");

        let deadline = Instant::now() + Duration::from_millis(500);
        let mut ticks = 0;
        let mut completions = [const { None }; 16];
        while ticks < 2 && Instant::now() < deadline {
            driver
                .wait(Some(Duration::from_millis(50)))
                .expect("wait interval");
            let n = driver.drain(&mut completions);
            ticks += completions[..n]
                .iter()
                .filter(|event| {
                    event
                        .as_ref()
                        .is_some_and(|event| event.route() == target().route())
                        && event
                            .as_ref()
                            .is_some_and(|event| event.result() != -libc::ECANCELED)
                })
                .count();
            completions[..n].fill_with(|| None);
        }
        assert!(ticks >= 2, "interval produced only {ticks} completion(s)");

        driver
            .push(Sqe::cancel(target(), kind::TIMER))
            .expect("cancel interval");

        #[cfg(target_os = "linux")]
        {
            let deadline = Instant::now() + Duration::from_millis(500);
            let mut cancelled = false;
            while !cancelled && Instant::now() < deadline {
                driver
                    .wait(Some(Duration::from_millis(50)))
                    .expect("wait cancellation");
                let n = driver.drain(&mut completions);
                cancelled = completions[..n].iter().any(|event| {
                    event.as_ref().is_some_and(|event| {
                        event.route() == target().route() && event.result() == -libc::ECANCELED
                    })
                });
                completions[..n].fill_with(|| None);
            }
            assert!(cancelled, "interval cancellation did not complete");
        }
    });
}

type Case = (&'static str, fn(&mut Snapshot), fn(Mismatch));

fn saturated_snapshot() -> Snapshot {
    let mut snap = Backend::snapshot().expect("detect");
    snap.rlimit_nofile = u64::MAX;
    snap.syncookies = true;
    snap.max_syn_backlog = u32::MAX;
    snap.somaxconn = u32::MAX;
    snap
}

#[test]
fn snapshot_detect_ok() {
    let snap = Backend::snapshot().expect("detect");
    assert!(snap.rlimit_nofile > 0);
    assert!(snap.somaxconn > 0);
}

#[test]
fn compat_check_baseline_cfg_passes_on_production_host() {
    let snap = Backend::snapshot().expect("detect");
    if !snap.syncookies && snap.max_syn_backlog < 4096 {
        return;
    }
    if snap.somaxconn < 4096 {
        return;
    }
    snap.check_slots(throughput_cfg().fixed_file_slots())
        .expect("baseline profile must pass on a properly tuned host");
}

#[test]
fn compat_check_rlimit_too_low_fails() {
    let cfg = throughput_cfg();
    let rows: [Case; 3] = [
        (
            "must reject 1-fd rlimit",
            |snap| snap.rlimit_nofile = 1,
            |err| match err {
                Mismatch::NoFileTooLow { rlimit, .. } => assert_eq!(rlimit, 1),
                other => panic!("expected NoFileTooLow, got {other:?}"),
            },
        ),
        (
            "must reject SYN-flood vulnerable host",
            |snap| {
                snap.syncookies = false;
                snap.max_syn_backlog = 128;
            },
            |err| match err {
                Mismatch::SynFloodVulnerable { backlog, .. } => assert_eq!(backlog, 128),
                other => panic!("expected SynFloodVulnerable, got {other:?}"),
            },
        ),
        (
            "must reject low somaxconn",
            |snap| snap.somaxconn = 128,
            |err| match err {
                Mismatch::SomaxconnTooLow { kernel, .. } => assert_eq!(kernel, 128),
                other => panic!("expected SomaxconnTooLow, got {other:?}"),
            },
        ),
    ];
    for (reject, degrade, verify) in rows {
        let mut snap = saturated_snapshot();
        degrade(&mut snap);
        verify(snap.check_slots(cfg.fixed_file_slots()).expect_err(reject));
    }
}

#[test]
fn mismatch_to_io_error() {
    let err: std::io::Error = Mismatch::NoFileTooLow {
        requested: 100,
        rlimit: 50,
    }
    .into();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    let msg = err.to_string();
    assert!(msg.contains("100"));
    assert!(msg.contains("50"));
}

#[test]
fn driver_does_not_raise_nofile() {
    let before = Backend::snapshot().expect("detect before").rlimit_nofile;
    let driver = Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver");
    let after = Backend::snapshot().expect("detect after").rlimit_nofile;
    assert_eq!(after, before);
    drop(driver);
}

#[test]
fn ready_batch_admission_is_fallible_and_atomic() {
    let mut config = driver::Config::for_quic_udp(1, 8);
    config.ready_slots = 2;
    let mut driver = pin!(Driver::new(config).expect("driver"));
    driver.as_mut().scope(|access, _| {
        let reference = access.driver_ref();
        let held = reference.make_ready_slot(target()).expect("first lease");
        let error = match reference.make_ready_slots([target().with_kind(1), target().with_kind(2)])
        {
            Ok(_) => panic!("oversized ready batch was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        drop(held);
        let batch = reference
            .make_ready_slots([target().with_kind(1), target().with_kind(2)])
            .expect("atomic failure must not consume a slot");
        assert_eq!(batch.len(), 2);
    });
}

#[test]
fn outbound_reservation_only_issues_slots_inside_its_range() {
    let mut driver = pin!(Driver::new(driver::Config::for_quic_udp(1, 8)).expect("driver"));
    driver.as_mut().scope(|mut access, _| {
        let reservation = access.reserve_outbound(1).expect("outbound reservation");
        assert!(reservation.slot(SlotIndex::new(0)).is_some());
        assert!(reservation.slot(SlotIndex::new(1)).is_none());
    });
}
