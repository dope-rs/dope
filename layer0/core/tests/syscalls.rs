use crate::common::file::cleanup;

use std::{
    env, fs,
    os::{fd::AsFd as _, unix::ffi::OsStrExt as _},
};

use dope_core::Backend;
use dope_sys::{io::file::Mode, kernel::errno::Source as _};

pub mod common;
use dope_core::{
    Errno,
    io::fs::{File, Len, Offset, open},
};
use dope_test::harness::support::file::Directory;

#[test]
fn preserves_file_ownership() {
    let base = env::temp_dir();
    let temp = Directory::try_with(&base, "syscalls", cleanup).expect("isolated directory");
    let mut path = temp.path().as_os_str().as_bytes().to_vec();
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
    let mut path = temp.path().as_os_str().as_bytes().to_vec();
    path.extend_from_slice(b"/missing");
    let path = std::ffi::CString::new(path).expect("child path");
    let failure = open::Options::new(open::Access::Read)
        .open(&path)
        .err()
        .expect("missing child must fail");
    Backend::clear();
    assert_eq!(failure, Errno::try_from(libc::ENOENT).expect("errno"));
}
