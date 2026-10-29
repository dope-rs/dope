use std::{env, fs, os::fd::AsFd as _, time};

use dope_core::Backend;
use dope_sys::{io::file::Mode, kernel::errno::Source as _};

use crate::common::file::NativePath as _;

pub mod common;
use common::file::Failure;
use dope_core::{
    Errno,
    io::fs::{File, Len, Offset, open},
};
use dope_test::harness::support::file::{Directory, Remove};

#[test]
fn preserves_file_ownership() {
    let base = env::temp_dir();
    let temp = Directory::try_with(&base, "syscalls", cleanup).expect("isolated directory");
    let mut path = temp.c_path().expect("directory path").into_bytes();
    path.extend_from_slice(b"/data");
    let path = std::ffi::CString::new(path).expect("child path");
    let file = open::Options::new(open::Access::ReadWrite)
        .creation(open::Creation::Exclusive)
        .mode(Mode::new(0o600))
        .open(&path)
        .expect("create data");
    let input = b"borrowed bytes";
    let written = file.try_write(input).expect("write data");
    assert_eq!(written, input);
    assert!(std::ptr::eq(written.as_ptr(), input.as_ptr()));
    file.try_sync().expect("sync data");
    let copy = {
        let read = open::Options::new(open::Access::Read)
            .open(&path)
            .expect("open reader");
        let mut output = [0xcc; 64];
        let pointer = output.as_mut_ptr();
        let bytes = read.try_read(&mut output).expect("read data");
        assert_eq!(bytes, input);
        assert!(std::ptr::eq(bytes.as_ptr(), pointer));
        assert_eq!(output[input.len()], 0xcc);
        assert!(read.try_read(&mut output).expect("read EOF").is_empty());
        File::from(read.as_fd().try_clone_to_owned().expect("duplicate reader"))
    };
    assert_eq!(
        copy.metadata()
            .expect("duplicate remains live")
            .len()
            .expect("nonnegative length"),
        Len::try_new(input.len() as u64).expect("bounded fixture length")
    );
    let file = fs::File::from(file);
    file.set_len(4).expect("truncate data");
    let file = File::from(file);
    assert_eq!(
        file.metadata()
            .expect("read size")
            .len()
            .expect("nonnegative length"),
        Len::fixed::<4>()
    );
}

#[test]
fn reads_at_without_moving_cursor() {
    let base = env::temp_dir();
    let temp = Directory::try_with(&base, "read_at", cleanup).expect("isolated directory");
    let path = temp.path().join("data");
    fs::write(&path, b"record\n").expect("fixture bytes");
    let file = File::from(fs::File::open(&path).expect("open reader"));
    let mut output = [0xcc; 5];
    assert_eq!(
        file.try_read_at(&mut output, Offset::fixed::<4>())
            .expect("positioned suffix"),
        b"rd\n"
    );
    assert_eq!(output, [b'r', b'd', b'\n', 0xcc, 0xcc]);
    assert!(
        file.try_read_at(&mut output, Offset::fixed::<7>())
            .expect("positioned EOF")
            .is_empty()
    );
    assert_eq!(
        file.try_read(&mut output[..1]).expect("unchanged cursor"),
        b"r"
    );
    let write = File::from(
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open writer"),
    );
    assert_eq!(
        write.try_read_at(&mut output, Offset::fixed::<0>()),
        Err(Errno::BAD_DESCRIPTOR)
    );
}

#[test]
fn open_keeps_failure() {
    let base = env::temp_dir();
    let temp = Directory::try_with(&base, "syscalls", cleanup).expect("isolated directory");
    let mut path = temp.c_path().expect("directory path").into_bytes();
    path.extend_from_slice(b"/missing");
    let path = std::ffi::CString::new(path).expect("child path");
    let failure = open::Options::new(open::Access::Read)
        .open(&path)
        .map_err(Failure::from)
        .err()
        .expect("missing child must fail");
    Backend::clear();
    assert_eq!(
        failure,
        Failure {
            errno: Errno::try_from(libc::ENOENT).expect("errno")
        }
    );
}

#[test]
fn polls_records_and_eof() {
    use std::iter;

    use dope_core::{
        dope_sys::io::socket::record::Record,
        platform::{channel::Channel, poll},
    };

    let (left, right) = Channel::pair().expect("connected records");
    let mut polls =
        poll::Set::try_new(iter::once(right.as_fd()), iter::empty()).expect("poll storage");
    assert_eq!(
        polls.poll(0).expect("idle poll").map(|ready| ready.index()),
        None
    );
    left.send(0x12345678).expect("send record");
    assert_eq!(
        polls
            .poll(0)
            .expect("ready poll")
            .map(|ready| ready.index()),
        Some(0)
    );
    assert!(matches!(right.recv(), Record::Value(0x12345678)));
    left.shutdown().expect("shutdown writer");
    assert!(matches!(right.recv(), Record::Closed));
}

#[test]
fn close_releases_endpoint() {
    use dope_core::{dope_sys::io::socket::record::Record, platform::channel::Channel};
    let right = {
        let pair = Channel::pair().expect("connected records");
        pair.1
    };
    assert!(matches!(right.recv(), Record::Closed));
}

#[test]
fn pipe_keeps_nonblocking() {
    use std::{io::Read as _, os::fd::AsRawFd as _};

    use dope_core::platform::wake::Ends;

    let mut output = [0; 16];
    let mut read = {
        let (read, write) = Ends::event().expect("open nonblocking pipe").split();
        for file in [read.as_fd(), write.as_fd()] {
            // SAFETY: descriptor flags are read from a live borrowed pipe endpoint.
            let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
            assert!(flags >= 0);
            assert_ne!(flags & libc::O_NONBLOCK, 0);
        }
        let mut read = fs::File::from(read);
        let empty = read
            .read(&mut output)
            .err()
            .map(|error| Errno::try_from(error).expect("native error").is_block());
        assert_eq!(empty, Some(true), "empty live pipe would block");
        write.notify().expect("write notification");
        assert_eq!(read.read(&mut output).expect("read pipe"), 1);
        assert_eq!(output[0], 1);
        read
    };
    assert_eq!(read.read(&mut output).expect("closed pipe reports EOF"), 0);
}

#[test]
fn wake_precedes_timeout() {
    use dope_core::platform::wake::{Ends, WaitError};

    let (wait, notify) = Ends::blocking().expect("notification pipe").split();
    for timeout in [time::Duration::ZERO, time::Duration::MAX] {
        notify.notify().expect("queued notification");
        assert_eq!(wait.wait_timeout(timeout), Ok(()));
    }
    assert!(matches!(
        wait.wait_timeout(time::Duration::ZERO),
        Err(WaitError::Timeout {
            timeout: time::Duration::ZERO,
            ..
        })
    ));
}

fn cleanup(result: Result<(), Remove>) {
    result.expect("fixture directory cleanup");
}
