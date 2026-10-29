use std::os::fd;

use dope_sys::io::{file::File, request::Request as _};
use dope_uring::Uring;

type Request<'a> = <Uring as File>::Request<'a>;

const _: for<'a> fn(fd::BorrowedFd<'a>) -> Request<'static> = |dir| Request::unlink(dir, c"entry");
