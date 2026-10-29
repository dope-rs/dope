use std::thread;

use dope_sys::{
    Timer as _,
    thread::{Thread, timer::Error},
};
use dope_uring::Uring;
use o3::base::time::Positive;

pub mod common;

use common::ring::seccomp;

#[test]
fn rejects_range() {
    let input = Positive::from_secs::<{ u64::MAX }>();
    assert!(matches!(
        <Uring as Thread>::Timer::try_new(input),
        Err(Error::Range { deadline }) if deadline == input
    ));
}

#[test]
fn retains_handle() {
    let timer = <Uring as Thread>::Timer::try_new(Positive::from_secs::<3600>())
        .expect("kernel watchdog creation");
    timer.try_refresh().expect("kernel watchdog refresh");
    timer.try_disarm().expect("kernel watchdog disarm");
    timer.try_refresh().expect("kernel watchdog rearm");
}

#[test]
fn rejects_arming() {
    let result = thread::spawn(|| {
        seccomp::deny_timer_settime().expect("thread-local timer-settime filter");
        <Uring as Thread>::Timer::try_new(Positive::from_secs::<3600>()).map(drop)
    })
    .join()
    .expect("watchdog rejection on the filtered thread");
    assert!(matches!(result, Err(Error::Arm(code)) if code.code().get() == libc::EPERM));
}

#[test]
fn rejects_exhaustion() {
    use std::{env, process};
    const CHILD: &str = "DOPE_TIMER_QUOTA_CHILD";
    if env::var_os(CHILD).is_some() {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: the initialized limit is borrowed only for this call in the child.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_SIGPENDING, &limit) },
            0
        );
        let fault = <Uring as Thread>::Timer::try_new(Positive::from_secs::<3600>())
            .err()
            .expect("exhausted watchdog quota");
        assert!(matches!(fault, Error::Create(code) if code.code().get() == libc::EAGAIN));
        return;
    }

    let program = env::current_exe().expect("locate the watchdog test process");
    let status = process::Command::new(program)
        .arg("--exact")
        .arg("rejects_exhaustion")
        .env(CHILD, "1")
        .status()
        .expect("start the watchdog quota test");
    assert!(status.success());
}
