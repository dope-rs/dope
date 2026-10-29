use std::os::fd;

use dope_sys::io::{file::File, request::Request as _};
use dope_uring::Uring;

type Request<'a> = <Uring as File>::Request<'a>;

fn escape<'a>(dir: fd::BorrowedFd<'a>) -> Request<'a> {
    let old = c"old".to_owned();
    Request::rename(dir, &old, c"new")
}
