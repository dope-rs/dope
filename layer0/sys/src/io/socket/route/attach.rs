use std::{convert, os::fd};

use crate::io::socket::route::{count::Index, install::Error};

/// Installs selection and per-socket admission for one routed group.
pub trait Attach: Sized {
    fn try_attach(&self, socket: fd::BorrowedFd<'_>) -> Result<(), Error>;
    fn try_filter(&mut self, key: Index, socket: fd::BorrowedFd<'_>) -> Result<(), Error>;
}

impl Attach for convert::Infallible {
    fn try_attach(&self, _: fd::BorrowedFd<'_>) -> Result<(), Error> {
        match *self {}
    }

    fn try_filter(&mut self, _: Index, _: fd::BorrowedFd<'_>) -> Result<(), Error> {
        match *self {}
    }
}
