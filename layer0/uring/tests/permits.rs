pub mod common;

use dope_sys::table::permit::{Create as _, Queue as _, Source, State};
use dope_uring::Uring;

use crate::common::ring::key::Key;

#[test]
fn grants_in_order() {
    let permit = <<Uring as Source>::Queue<Key, u32>>::try_acquire()
        .expect("native receive permits are available");
    let registry = <<Uring as Source>::Queue<Key, u32>>::try_new(4)
        .expect("the four-entry permit registry must allocate");

    assert!(registry.try_arm(&permit, 2, Key(12)).is_ok());
    assert!(registry.try_arm(&permit, 0, Key(10)).is_ok());
    assert!(registry.try_arm(&permit, 3, Key(13)).is_ok());
    assert!(registry.cancel(&permit, 0, Key(10)) == State::Waiting);
    assert!(registry.grant_one(&permit) == Some((2, Key(12))));
    assert!(registry.grant_one(&permit) == Some((3, Key(13))));
    assert!(registry.grant_one(&permit).is_none());
    assert!(registry.cancel(&permit, 2, Key(12)) == State::Granted(Key(12)));
    assert!(registry.take(&permit, 3, Key(13)) == State::Granted(Key(13)));
}

#[test]
fn take_is_exact() {
    let permit = <<Uring as Source>::Queue<Key, u32>>::try_acquire()
        .expect("native receive permits are available");
    let registry = <<Uring as Source>::Queue<Key, u32>>::try_new(1)
        .expect("the one-entry permit registry must allocate");

    assert!(registry.try_arm(&permit, 0, Key(1)).is_ok());
    assert!(registry.take(&permit, 0, Key(1)) == State::Waiting);
    assert!(registry.try_arm(&permit, 0, Key(2)) == Err(Key(2)));
    assert!(registry.cancel(&permit, 0, Key(2)) == State::Stale);
    assert!(registry.take(&permit, 0, Key(2)) == State::Stale);
    assert!(registry.grant_one(&permit) == Some((0, Key(1))));
    assert!(registry.cancel(&permit, 0, Key(2)) == State::Stale);
    assert!(registry.take(&permit, 0, Key(2)) == State::Stale);
    assert!(registry.take(&permit, 0, Key(1)) == State::Granted(Key(1)));
    assert!(registry.take(&permit, 0, Key(1)) == State::Gone);
    assert!(registry.try_arm(&permit, 0, Key(2)).is_ok());
    assert!(registry.cancel(&permit, 0, Key(2)) == State::Waiting);
    assert!(registry.cancel(&permit, 0, Key(2)) == State::Gone);
    assert!(registry.take(&permit, 0, Key(2)) == State::Gone);
}
