use std::{io, num, ptr};

use dope_sys::{Errno, kernel, kernel::raw::Status};

#[test]
fn keeps_positive_codes() {
    for raw in [1, libc::EBADF, libc::EINTR, i32::MAX] {
        assert_eq!(
            Errno::try_from(raw).expect("positive errno").code().get(),
            raw
        );
        let code = num::NonZero::new(raw).expect("the fixture contains positive errno codes");
        let errno = Errno::try_from(code).expect("positive errno");
        assert_eq!(errno.code(), code);
        let fault = kernel::Errno::<()>::from(errno);
        assert_eq!(Errno::from(fault).code(), code);
    }
}

#[test]
fn pthread_returns_code() {
    assert_eq!(unsafe { libc::close(-1) }, -1);
    assert_eq!(unsafe { Status::new(0).try_pthread() }, Ok(()));
    assert_eq!(
        unsafe { Status::new(libc::EINTR).try_pthread() },
        Err(Errno::INTERRUPTED),
    );
}

#[test]
fn rejects_nonpositive() {
    for raw in [i32::MIN, -1, 0] {
        let error = kernel::Invalid {
            actual: raw as libc::ssize_t,
            minimum: 1,
            maximum: i32::MAX as libc::ssize_t,
        };
        assert_eq!(Errno::try_from(raw), Err(error));
        if let Some(code) = num::NonZero::new(raw) {
            assert_eq!(Errno::try_from(code), Err(error));
        }
    }
}

#[test]
fn keeps_std_error_owner() {
    let error = io::Error::from(Errno::INTERRUPTED);
    assert_eq!(
        Errno::try_from(error).expect("native errno"),
        Errno::INTERRUPTED,
    );
    for raw in [0, -1] {
        let error =
            Errno::try_from(io::Error::from_raw_os_error(raw)).expect_err("nonpositive errno");
        assert_eq!(error.raw_os_error(), Some(raw));
    }
    let error = io::Error::other("retained payload");
    let payload = ptr::from_ref(error.get_ref().expect("payload"));
    let error = Errno::try_from(error).expect_err("non-native error");
    assert!(ptr::eq(
        payload,
        ptr::from_ref(error.get_ref().expect("payload")),
    ));
}
