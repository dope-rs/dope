mod ring;

use std::os::{fd, fd::AsFd as _};

use dope_sys::io::reactor::submit::{Control, Poll as _};
use dope_uring::Uring;
use ring::Token;

type Poll<'a> = <Uring as Control>::Poll<'a, Token>;

fn escape<'a>(file: fd::OwnedFd) -> Poll<'a> {
    Poll::new(file.as_fd(), Token(0))
}
