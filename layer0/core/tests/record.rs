use std::os::fd::AsFd as _;

use dope_core::{
    dope_sys::io::socket::record::{Fault, Record},
    platform::channel::Channel,
};

pub mod common;
use common::rt::record::Socket;

#[test]
fn pair_preserves_records() {
    let (left, right) = Channel::pair().expect("record socket pair");
    assert!(Socket::new(left.as_fd()).try_cloexec().expect("left flags"));
    assert!(
        Socket::new(right.as_fd())
            .try_cloexec()
            .expect("right flags")
    );
    left.send(7).expect("first record");
    left.send(19).expect("second record");
    assert!(matches!(right.recv(), Record::Value(7)));
    assert!(matches!(right.recv(), Record::Value(19)));
    left.shutdown().expect("shutdown");
    assert!(matches!(right.recv(), Record::Closed));
}

#[test]
fn rejects_truncated_record() {
    let (left, right) = Channel::pair().expect("record socket pair");
    Socket::new(left.as_fd())
        .send(b"x")
        .expect("short native record");
    left.send(19).expect("following record");
    let size = match right.recv() {
        Record::Failed(Fault::Record(size)) => Some((size.expected, size.actual)),
        _ => None,
    };
    assert_eq!(size, Some((4, 1)));
    assert!(matches!(right.recv(), Record::Value(19)));
}

#[test]
fn rejects_oversized_record() {
    let (left, right) = Channel::pair().expect("record socket pair");
    Socket::new(left.as_fd())
        .send(&[0u8; 8])
        .expect("long native record");
    left.send(19).expect("following record");
    let size = match right.recv() {
        Record::Failed(Fault::Record(size)) => Some((size.expected, size.actual)),
        _ => None,
    };
    assert_eq!(size, Some((4, 5)));
    assert!(matches!(right.recv(), Record::Value(19)));
}
