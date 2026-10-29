use std::{iter, os::fd::AsFd};

use dope_core::platform::{channel::Channel, poll::Set};

#[test]
fn retire_keeps_indices() {
    let (closed, first) = Channel::pair().expect("first channel");
    let (sender, second) = Channel::pair().expect("second channel");
    let mut set = Set::try_new(iter::once(first.as_fd()), iter::once(second.as_fd()))
        .expect("poll registrations");
    drop(closed);
    sender.send(7).expect("second notification");
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
