use std::{convert::Infallible, mem::MaybeUninit, ptr};

use dope_core::{dope_sys::thread::signal::Signal as _, platform::signal::Source};
use o3::mem::slot;

fn main() {
    let mut before = MaybeUninit::<libc::sigaction>::uninit();
    let mut mask = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: null inputs query this single-threaded process's existing policy.
    unsafe {
        assert_eq!(
            libc::sigaction(libc::SIGINT, ptr::null(), before.as_mut_ptr()),
            0
        );
        assert_eq!(
            libc::pthread_sigmask(libc::SIG_BLOCK, ptr::null(), mask.as_mut_ptr()),
            0
        );
    }
    let mut retained = MaybeUninit::uninit();
    Source::scope(slot::Vacant::new(&mut retained), |source| {
        // SAFETY: this isolated process owns the acquired native signal source.
        assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGINT) }, 0);
        source.wait().expect("interrupt received");
        Ok::<_, Infallible>(())
    })
    .expect("interrupt cleanup");

    let mut after = MaybeUninit::<libc::sigaction>::uninit();
    let mut restored = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: the writable outputs are initialized by these successful queries.
    unsafe {
        assert_eq!(
            libc::sigaction(libc::SIGINT, ptr::null(), after.as_mut_ptr()),
            0
        );
        assert_eq!(
            libc::pthread_sigmask(libc::SIG_BLOCK, ptr::null(), restored.as_mut_ptr()),
            0
        );
        let (before, after) = (before.assume_init(), after.assume_init());
        assert_eq!(before.sa_sigaction, after.sa_sigaction);
        assert_eq!(before.sa_flags, after.sa_flags);
        for signal in [libc::SIGTERM, libc::SIGINT] {
            assert_eq!(
                libc::sigismember(&before.sa_mask, signal),
                libc::sigismember(&after.sa_mask, signal)
            );
            assert_eq!(
                libc::sigismember(mask.as_ptr(), signal),
                libc::sigismember(restored.as_ptr(), signal)
            );
        }
    }
}
