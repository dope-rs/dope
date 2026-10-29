mod ring;

use std::os::{fd, fd::AsFd as _};

use dope_sys::io::reactor::submit::{Control, Poll as _};
use dope_uring::Uring;
use ring::Token;

type Poll<'a> = <Uring as Control>::Poll<'a, Token>;

fn release(file: fd::OwnedFd) {
    let poll = Poll::new(file.as_fd(), Token(0));
    drop(file);
    let _ = poll.entry();
}
