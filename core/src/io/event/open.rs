use std::{mem, num, os::fd, process};

use crate::{driver::route, io::event};

#[repr(transparent)]
pub struct Opened(fd::OwnedFd);

impl Opened {
    pub(crate) fn new(fd: fd::OwnedFd) -> Self {
        Self(fd)
    }

    pub fn into_owned(self) -> fd::OwnedFd {
        self.0
    }
}

pub enum Outcome {
    Opened(Opened),
    Failed(i32),
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Error(num::NonZeroI32);

impl Error {
    pub(crate) fn from_errno(errno: i32) -> Self {
        let Some(errno) = num::NonZeroI32::new(errno) else {
            process::abort();
        };
        if errno.get() < 0 {
            process::abort();
        }
        Self(errno)
    }

    pub(in crate::io) const fn get(self) -> i32 {
        self.0.get()
    }
}

const _: () = assert!(mem::size_of::<Error>() == mem::size_of::<i32>());

/// An open completion and its affine driver target.
///
/// Safe code cannot mint completion authority.
///
/// ```compile_fail
/// use dope_core::{driver::route, io::event::open};
///
/// let _ = open::Completion::new(route::SHUTDOWN, open::Outcome::Failed(1));
/// ```
#[repr(transparent)]
pub struct Completion {
    targeted: event::Targeted<Outcome>,
}

const _: () = {
    assert!(mem::size_of::<Completion>() == mem::size_of::<(route::Token, Outcome)>());
    assert!(mem::align_of::<Completion>() == mem::align_of::<(route::Token, Outcome)>());
};

impl Completion {
    pub(in crate::io) const fn new(token: route::Token, outcome: Outcome) -> Self {
        Self {
            targeted: event::Targeted::new(token, outcome),
        }
    }

    pub const fn token(&self) -> route::Token {
        self.targeted.token()
    }

    pub const fn outcome(&self) -> &Outcome {
        self.targeted.value()
    }

    pub fn into_parts(self) -> (route::Token, Outcome) {
        self.targeted.into_parts()
    }
}
