use std::{iter, os::fd::AsFd};

use dope_core::{Backend, Errno, platform::poll::Set};
use dope_sys::io::{fd::Fd as _, pipe::Pipe as _};

#[test]
fn retire_keeps_indices() {
    let (first, closed) = Backend::nonblocking().expect("first pipe");
    let (second, sender) = Backend::nonblocking().expect("second pipe");
    let mut set = Set::try_new(iter::once(first.as_fd()), iter::once(second.as_fd()))
        .expect("poll registrations");
    assert_eq!(
        set.poll(0).expect("idle poll").map(|ready| ready.index()),
        None
    );
    drop(closed);
    Backend::write::<Errno>(sender.as_fd(), &[1]).expect("second notification");
    let ready = set
        .poll(0)
        .expect("closed readiness")
        .expect("closed entry");
    assert_eq!(ready.index(), 0);
    ready.retire();
    assert_eq!(
        set.poll(0)
            .expect("remaining readiness")
            .map(|ready| ready.index()),
        Some(1)
    );
}
