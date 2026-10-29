use std::{convert, os::fd};

use crate::io::socket::route::{count::Index, install::Error};

/// Owns the native program and map while building one routed socket group.
pub trait Attach: Sized {
    fn try_attach(&self, socket: fd::BorrowedFd<'_>) -> Result<(), Error>;
    fn try_insert(self, key: Index, socket: fd::BorrowedFd<'_>) -> Result<Self, Error>;
}

impl Attach for convert::Infallible {
    fn try_attach(&self, _: fd::BorrowedFd<'_>) -> Result<(), Error> {
        match *self {}
    }

    fn try_insert(self, _: Index, _: fd::BorrowedFd<'_>) -> Result<Self, Error> {
        match self {}
    }
}
