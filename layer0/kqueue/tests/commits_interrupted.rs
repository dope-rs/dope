pub mod common;

use std::{
    io::{self, Read as _, Write as _},
    mem,
    num::NonZeroUsize,
    os::unix::net::UnixStream,
    ptr, thread, time,
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

#[test]
fn commits_interrupted() {
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
    let mut previous_action = mem::MaybeUninit::uninit();
    // SAFETY: this sole test owns the disposition and retains its previous value.
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGALRM, &action, previous_action.as_mut_ptr()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    // SAFETY: the successful call initialized the previous disposition.
    let previous_action = unsafe { previous_action.assume_init() };
    let mut previous_mask = mem::MaybeUninit::uninit();
    // SAFETY: only this waiting test thread's mask is changed and retained.
    assert_eq!(
        unsafe {
            libc::pthread_sigmask(
                libc::SIG_UNBLOCK,
                &action.sa_mask,
                previous_mask.as_mut_ptr(),
            )
        },
        0
    );
    // SAFETY: the successful call initialized the previous thread mask.
    let previous_mask = unsafe { previous_mask.assume_init() };
    let (stop, mut stopped) = UnixStream::pair().expect("signal sender lifetime");
    stopped
        .set_read_timeout(Some(time::Duration::from_millis(20)))
        .expect("bounded signal retry");
    // SAFETY: the scoped sender joins before this waiting thread can return.
    let waiting = unsafe { libc::pthread_self() } as usize;
    let (result, errno) = thread::scope(|scope| {
        let sender = scope.spawn(move || {
            // Repeated directed delivery closes the wait-entry race; the sender is bounded.
            for _ in 0..250 {
                match stopped.read(&mut [0]) {
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) =>
                    {
                        // SAFETY: the waiting thread outlives this sender and owns SIGALRM.
                        assert_eq!(
                            unsafe {
                                libc::pthread_kill(waiting as libc::pthread_t, libc::SIGALRM)
                            },
                            0
                        );
                    }
                    result => {
                        assert_eq!(result.expect("signal sender termination"), 0);
                        break;
                    }
                }
            }
        });
        // SAFETY: __error names this thread's errno; no handler or commit changes it.
        unsafe { *libc::__error() = 0 };
        let result = queue.wait(
            &mut files,
            &mut storage,
            1,
            Some(time::Duration::from_secs(5)),
            &mut budget,
        );
        // SAFETY: capture the result before the sender's shutdown changes errno.
        let errno = unsafe { *libc::__error() };
        // SAFETY: block this thread's owned signal before stopping its sender.
        assert_eq!(
            unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &action.sa_mask, ptr::null_mut()) },
            0
        );
        drop(stop);
        sender.join().expect("joined signal sender");
        (result, errno)
    });
    let mut pending = mem::MaybeUninit::uninit();
    // SAFETY: the output owns one signal set and the signal remains blocked.
    assert_eq!(
        unsafe { libc::sigpending(pending.as_mut_ptr()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    // SAFETY: the successful query initialized the set.
    let pending = unsafe { pending.assume_init() };
    // SAFETY: SIGALRM is valid and the initialized set stays borrowed.
    let pending = unsafe { libc::sigismember(&pending, libc::SIGALRM) };
    assert!(matches!(pending, 0 | 1));
    if pending == 1 {
        let mut signal = 0;
        // SAFETY: the pending signal is blocked and its sender has joined.
        assert_eq!(unsafe { libc::sigwait(&action.sa_mask, &mut signal) }, 0);
        assert_eq!(signal, libc::SIGALRM);
    }
    // SAFETY: no signal sender remains and the test restores the saved disposition.
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGALRM, &previous_action, ptr::null_mut()) },
        0,
        "{}",
        io::Error::last_os_error()
    );
    // SAFETY: pending test delivery was drained before restoring the original mask.
    assert_eq!(
        unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &previous_mask, ptr::null_mut()) },
        0
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
