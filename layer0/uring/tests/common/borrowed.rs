mod ring;

use std::os::{fd, fd::AsFd as _};

use dope_sys::{
    io::{
        file::File,
        reactor::submit::{Control, Poll as _},
        request::{Borrow, Request as _},
        socket::addr,
    },
    memory::{self, buffer::provided},
    probe::{self, session},
    table::{permit, token},
};
use dope_uring::Uring;
use ring::{Token, key::Lifetime};

type Request<'a> = <Uring as File>::Request<'a>;
type Poll<'a> = <Uring as Control>::Poll<'a, Token>;
type Queue<'a> = <Uring as permit::Source>::Queue<Lifetime<'a>, u32>;
type Direct<T, L> = <<Uring as probe::Source>::Session<T, L> as session::Empty>::Direct;

const _: for<'a> fn(<Request<'a> as Borrow<'a>>::Target, addr::Addr<'a>) -> Request<'a> =
    |slot, address| Request::connect(slot, address);
const _: for<'a> fn(fd::BorrowedFd<'a>, &'a <Uring as File>::Options) -> Request<'a> =
    |dir, options| Request::open(dir, c"entry", options);
const _: for<'a> fn(fd::BorrowedFd<'a>, &'a std::ffi::CStr) -> Request<'a> =
    |dir, old| Request::rename(dir, old, c"new");
const _: for<'a> fn(fd::BorrowedFd<'a>) -> Request<'a> = |dir| Request::unlink(dir, c"entry");
const _: for<'a> fn(&'a fd::OwnedFd) -> Poll<'a> = |file| Poll::new(file.as_fd(), Token(0));
const _: for<'a> fn(&'a fd::OwnedFd) -> Poll<'a> = |file| Poll::multi(file.as_fd(), Token(0));

fn retain_fifo<'a>(fifo: Queue<'a>) -> Queue<'a> {
    fifo
}

fn retain<E: memory::raw::Extent + Copy>(
    token: <Uring as provided::Source>::Pending<E>,
) -> <Uring as provided::Source>::Pending<E> {
    token
}

fn cancel<T: token::Codec, L: memory::raw::Canary>() {
    let _ = |state: <Direct<T, L> as session::Session>::Pending| session::Pending::cancel(state);
}

fn retain_poll(file: fd::OwnedFd) {
    let poll = Poll::new(file.as_fd(), Token(0));
    let _ = poll.entry();
    drop(poll);
    let _ = file.as_fd();
    drop(file);
}
