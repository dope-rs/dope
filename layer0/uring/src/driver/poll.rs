use std::{mem, os::fd};

use dope_sys::engine::register;
use dope_sys::io::reactor::{complete, submit};

use crate::reactor::{op::controls, sq};

/// Retains its shutdown source borrow and stays on its owning thread.
#[repr(transparent)]
pub struct Poll<'a, T> {
    entry: sq::Submit<'a, T>,
}

impl<'a, T: complete::raw::Token> submit::Poll<'a, T> for Poll<'a, T> {
    type Entry = sq::Submit<'a, T>;

    fn new(source: register::Source<'a>, token: T) -> Self {
        let entry = controls::poll(source, false).user_data(token);
        Self { entry }
    }

    fn multi(file: fd::BorrowedFd<'a>, token: T) -> Self {
        let entry = controls::poll(
            register::Source {
                fd: file,
                interest: register::Interest::Readable,
            },
            true,
        )
        .user_data(token);
        Self { entry }
    }

    fn entry(&self) -> &Self::Entry {
        &self.entry
    }
}

const _: () = {
    assert!(mem::size_of::<Poll<'static, u64>>() == mem::size_of::<sq::Submit<'static, u64>>());
};
