use dope_sys::io::{
    file::File,
    request::{Borrow, Request as _},
    socket::addr,
};
use dope_uring::Uring;

type Request<'a> = <Uring as File>::Request<'a>;

const _: for<'a> fn(<Request<'a> as Borrow<'a>>::Target, addr::Addr<'a>) -> Request<'static> =
    |slot, address| Request::connect(slot, address);
