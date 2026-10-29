use std::os::fd;

use crate::io::reactor::complete;

/// A shutdown poll retaining the exact source descriptor and thread affinity.
pub trait Poll<'a, T: complete::raw::Token>: Sized {
    type Entry;

    fn new(file: fd::BorrowedFd<'a>, token: T) -> Self;
    fn multi(file: fd::BorrowedFd<'a>, token: T) -> Self;
    fn entry(&self) -> &Self::Entry;
}
