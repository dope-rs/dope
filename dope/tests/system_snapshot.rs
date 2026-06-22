use dope::DriverConfig;
use dope::platform::{Mismatch, Snapshot};
use dope::runtime::profile;

#[test]
fn snapshot_detect_ok() {
    let snap = Snapshot::detect().expect("detect");
    assert!(snap.rlimit_nofile > 0);
    assert!(snap.somaxconn > 0);
}

#[test]
fn compat_check_baseline_cfg_passes_on_production_host() {
    let snap = Snapshot::detect().expect("detect");
    if !snap.syncookies && snap.max_syn_backlog < 4096 {
        return;
    }
    if snap.somaxconn < 4096 {
        return;
    }
    let cfg = dope::DriverCfg::for_profile::<profile::Throughput>();
    snap.check(&cfg)
        .expect("baseline profile must pass on a properly tuned host");
}

#[test]
fn compat_check_rlimit_too_low_fails() {
    let mut snap = Snapshot::detect().expect("detect");
    let cfg = dope::DriverCfg::for_profile::<profile::Throughput>();
    snap.rlimit_nofile = 1;
    snap.syncookies = true;
    snap.somaxconn = 65535;
    let err = snap.check(&cfg).expect_err("must reject 1-fd rlimit");
    match err {
        Mismatch::NoFileTooLow { rlimit, .. } => assert_eq!(rlimit, 1),
        other => panic!("expected NoFileTooLow, got {other:?}"),
    }
}

#[test]
fn compat_check_syn_flood_vulnerable_fails() {
    let mut snap = Snapshot::detect().expect("detect");
    let cfg = dope::DriverCfg::for_profile::<profile::Throughput>();
    snap.rlimit_nofile = u64::MAX;
    snap.syncookies = false;
    snap.max_syn_backlog = 128;
    snap.somaxconn = 65535;
    let err = snap
        .check(&cfg)
        .expect_err("must reject SYN-flood vulnerable host");
    match err {
        Mismatch::SynFloodVulnerable { backlog, .. } => assert_eq!(backlog, 128),
        other => panic!("expected SynFloodVulnerable, got {other:?}"),
    }
}

#[test]
fn compat_check_somaxconn_too_low_fails() {
    let mut snap = Snapshot::detect().expect("detect");
    let cfg = dope::DriverCfg::for_profile::<profile::Throughput>();
    snap.rlimit_nofile = u64::MAX;
    snap.syncookies = true;
    snap.max_syn_backlog = u32::MAX;
    snap.somaxconn = 128;
    let err = snap.check(&cfg).expect_err("must reject low somaxconn");
    match err {
        Mismatch::SomaxconnTooLow { kernel, .. } => assert_eq!(kernel, 128),
        other => panic!("expected SomaxconnTooLow, got {other:?}"),
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
