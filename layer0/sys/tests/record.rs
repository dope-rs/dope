use dope_sys::{
    Errno,
    io::socket::record::{Fault, Record, Stream, Transport as _},
};

pub mod common;
use common::record::{Transfer, frame};

#[test]
fn stream_partial_io() {
    let value = 0x12345678u32;
    let mut writer = Transfer::new(
        &[],
        &[
            Ok(1),
            Err(Errno::INTERRUPTED),
            Ok(2),
            Err(Errno::INTERRUPTED),
            Ok(1),
        ],
    );
    Stream::send(value, |input| writer.send(input)).expect("complete record");
    assert_eq!(
        writer.sent,
        frame(&value.to_ne_bytes()).expect("bounded frame")
    );

    let mut reader = Transfer::new(
        &writer.sent,
        &[
            Err(Errno::INTERRUPTED),
            Ok(1),
            Ok(2),
            Err(Errno::INTERRUPTED),
            Ok(1),
        ],
    );
    assert!(matches!(
        Stream::recv(|output| reader.recv(output)),
        Record::Value(received) if received == value
    ));
}

#[test]
fn stream_malformed_drain() {
    let mut bytes = frame(&[]).expect("empty frame");
    bytes.extend_from_slice(&frame(&[0; u8::MAX as usize]).expect("maximum frame"));
    bytes.extend_from_slice(&frame(&19u32.to_ne_bytes()).expect("bounded frame"));
    let mut reader = Transfer::new(&bytes, &[]);
    for actual in [0, 5] {
        let size = match Stream::recv(|output| reader.recv(output)) {
            Record::Failed(Fault::Record(size)) => Some((size.expected, size.actual)),
            _ => None,
        };
        assert_eq!(size, Some((4, actual)));
    }
    assert!(matches!(
        Stream::recv(|output| reader.recv(output)),
        Record::Value(19)
    ));
}

#[test]
fn stream_partial_eof() {
    let mut reader = Transfer::new(&[], &[]);
    assert!(matches!(
        Stream::recv(|output| reader.recv(output)),
        Record::Closed
    ));
    for (bytes, actual) in [(&[4][..], 0), (&[4, 7, 8][..], 2), (&[8, 7, 8][..], 2)] {
        let mut reader = Transfer::new(bytes, &[]);
        let size = match Stream::recv(|output| reader.recv(output)) {
            Record::Failed(Fault::Record(size)) => Some((size.expected, size.actual)),
            _ => None,
        };
        assert_eq!(size, Some((4, actual)));
    }
}

#[test]
fn stream_syscall_error() {
    let mut writer = Transfer::new(&[], &[Ok(2), Err(Errno::BAD_DESCRIPTOR)]);
    assert!(matches!(
        Stream::send(7, |input| writer.send(input)),
        Err(Fault::Syscall(Errno::BAD_DESCRIPTOR))
    ));

    let bytes = frame(&7u32.to_ne_bytes()).expect("bounded frame");
    let mut reader = Transfer::new(&bytes, &[Ok(1), Ok(2), Err(Errno::NO_BUFFER_SPACE)]);
    assert!(matches!(
        Stream::recv(|output| reader.recv(output)),
        Record::Failed(Fault::Syscall(Errno::NO_BUFFER_SPACE))
    ));
}

#[test]
fn stream_bad_completions() {
    let mut writer = Transfer::new(&[], &[Ok(2), Ok(0)]);
    let size = match Stream::send(7, |input| writer.send(input)) {
        Err(Fault::Record(size)) => Some((size.expected, size.actual)),
        _ => None,
    };
    assert_eq!(size, Some((4, 1)));
    let size = match Stream::send(7, |_| Ok(b"excess")) {
        Err(Fault::Record(size)) => Some((size.expected, size.actual)),
        _ => None,
    };
    assert_eq!(size, Some((4, 5)));
}
