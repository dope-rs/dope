pub mod common;

use std::{
    io::{self, Write as _},
    mem,
    num::NonZeroUsize,
    ptr, time,
};

use dope_sys::{
    io::reactor::ready::{Poll as _, delivery::Event as _},
    table::{
        Files as _,
        change::{Changes as _, Interest as _},
    },
};
use o3::mem::quota;

use common::{Budget, Files, Poll, slots::inputs::Layout, socket::try_pair};

fn main() {
    let mut queue = Poll::try_new().expect("kqueue creation");
    let mut files = Files::try_new(Layout).expect("file table");
    let (handle, mut peer) = try_pair().expect("socket pair");
    let accepted = files.establish(handle).expect("accepted socket");
    files
        .try_read(accepted.physical())
        .expect("socket interest")
        .arm(libc::EV_ADD | libc::EV_CLEAR, NonZeroUsize::MIN);
    let mut storage = Poll::storage();
    let source = quota::Ledger::new(1);
    let mut budget = Budget::new(&source);

    // SAFETY: zero initializes Darwin's sigaction, and the C handler borrows no state.
    let mut action: libc::sigaction = unsafe { mem::zeroed() };
    action.sa_sigaction = common::caught as *const () as libc::sighandler_t;
    // SAFETY: the initialized action owns writable mask storage in this isolated process.
    assert_eq!(unsafe { libc::sigemptyset(&mut action.sa_mask) }, 0);
    // SAFETY: SIGALRM is a valid signal and the mask remains writable.
    assert_eq!(
        unsafe { libc::sigaddset(&mut action.sa_mask, libc::SIGALRM) },
        0
    );
    // SAFETY: this process owns its disposition until exit; no handler state can escape.
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGALRM, &action, ptr::null_mut()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    // SAFETY: only this isolated process's main-thread mask is changed.
    assert_eq!(
        unsafe { libc::sigprocmask(libc::SIG_UNBLOCK, &action.sa_mask, ptr::null_mut()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    let tick = libc::timeval {
        tv_sec: 0,
        tv_usec: 20_000,
    };
    let mut timer = libc::itimerval {
        it_interval: tick,
        it_value: tick,
    };
    // SAFETY: this process owns the timer; repeating delivery closes the wait-entry race.
    assert_eq!(
        unsafe { libc::setitimer(libc::ITIMER_REAL, &timer, ptr::null_mut()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    // SAFETY: __error names this thread's errno; no handler or commit changes it.
    unsafe { *libc::__error() = 0 };
    let result = queue.wait(
        &mut files,
        &mut storage,
        1,
        Some(time::Duration::from_secs(5)),
        &mut budget,
    );
    // SAFETY: capture the native result before any subsequent syscall can change errno.
    let errno = unsafe { *libc::__error() };
    timer.it_value.tv_usec = 0;
    // SAFETY: zero initial expiry disarms this process's owned interval timer.
    assert_eq!(
        unsafe { libc::setitimer(libc::ITIMER_REAL, &timer, ptr::null_mut()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    assert_eq!(
        errno,
        libc::EINTR,
        "wait must be interrupted, not timed out"
    );
    assert!(
        result
            .expect("interrupted registration wait")
            .next()
            .is_none()
    );
    assert_eq!(queue.check(), Ok(()));
    assert_eq!(files.changes().len(), 0);
    assert_eq!(dope_sys::Budget::remaining(&budget), 0);

    peer.write_all(b"x").expect("ready peer");
    let mut events = queue
        .wait(
            &mut files,
            &mut storage,
            1,
            Some(time::Duration::ZERO),
            &mut budget,
        )
        .expect("previously applied registration");
    let event = events.next().expect("retained socket readiness");
    assert_eq!(event.filter(), libc::EVFILT_READ);
    assert_eq!(event.error(), None);
    assert_eq!(event.data(), 1);
    assert!(events.next().is_none());
}
