use std::{mem::MaybeUninit, ptr};

use dope_sys::thread::{Thread, signal::raw::Receiver as _};
use dope_uring::Uring;

#[test]
fn restores_mask() {
    let mut before = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: null input only queries this thread's mask into writable storage.
    assert_eq!(
        unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, ptr::null(), before.as_mut_ptr()) },
        0
    );
    // SAFETY: this executable has one SIGTERM policy owner, sends no signals,
    // and starts no workers inheriting the acquired policy.
    let receiver = unsafe { <Uring as Thread>::Receiver::try_new() }.expect("signal receiver");
    let mut blocked = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: null input queries the acquired mask into writable storage.
    assert_eq!(
        unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, ptr::null(), blocked.as_mut_ptr()) },
        0
    );
    // SAFETY: the successful query initialized blocked and SIGTERM is valid.
    assert_eq!(
        unsafe { libc::sigismember(blocked.as_ptr(), libc::SIGTERM) },
        1
    );
    receiver.restore().expect("restore original mask");
    let mut after = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: null input queries the restored mask into writable storage.
    assert_eq!(
        unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, ptr::null(), after.as_mut_ptr()) },
        0
    );
    for signal in 1..=libc::SIGRTMAX() {
        // SAFETY: both successful queries initialized their signal sets.
        assert_eq!(
            unsafe { libc::sigismember(before.as_ptr(), signal) },
            unsafe { libc::sigismember(after.as_ptr(), signal) }
        );
    }
}
