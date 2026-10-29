use dope_sys::{Errno, io::socket::accept::Source as _};
use dope_uring::Uring;

#[test]
fn classifies_failures() {
    for (code, retry) in [
        (libc::EINTR, true),
        (libc::ECONNABORTED, true),
        (libc::EAGAIN, true),
        (libc::ENETDOWN, true),
        (libc::EPROTO, true),
        (libc::ENOPROTOOPT, true),
        (libc::EHOSTDOWN, true),
        (libc::ENONET, true),
        (libc::EHOSTUNREACH, true),
        (libc::EOPNOTSUPP, true),
        (libc::ENETUNREACH, true),
        (libc::EMFILE, false),
        (libc::ENFILE, false),
        (libc::ENOMEM, false),
        (libc::ENOBUFS, false),
        (libc::EBADF, false),
        (libc::EINVAL, false),
        (libc::ECANCELED, false),
    ] {
        let error = Errno::try_from(code).expect("native errno");
        assert_eq!(Uring::is_retryable(error), retry, "{error:?}");
        assert_eq!(Uring::can_defer(error), code == libc::ENFILE, "{error:?}");
    }
}
