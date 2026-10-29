use std::os::fd;

use dope_sys::io::{file, file::File, request::Request as _};
use dope_uring::Uring;

type Request<'a> = <Uring as File>::Request<'a>;

fn escape<'a>(dir: fd::BorrowedFd<'a>) -> Request<'a> {
    let options = Request::options(file::Open::READ, file::Mode::new(0));
    Request::open(dir, c"entry", &options)
}
