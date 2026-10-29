use std::{mem, os::fd};

use dope_sys::{
    Errno,
    io::{
        socket,
        socket::{Socket as _, addr},
    },
    probe::{Creation, receive},
};
use fd::AsFd as _;

use crate::{Uring, sockets::probe};

pub(crate) struct Pair {
    reader: fd::OwnedFd,
    writer: fd::OwnedFd,
}

impl fd::AsFd for Pair {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.reader.as_fd()
    }
}

impl Pair {
    pub(crate) fn new() -> Result<Self, Creation> {
        let kind = socket::Kind::Dgram.raw() | libc::SOCK_NONBLOCK;
        let reader = probe::bind(kind)?;
        let writer = probe::bind(kind)?;
        let mut storage = addr::Storage::<addr::Length>::empty();
        let address = addr::Addr::local::<Uring, Errno>(reader.as_fd(), &mut storage)?;
        address.try_connect::<Uring, Errno>(writer.as_fd())?;
        Ok(Self { reader, writer })
    }

    pub(crate) fn send(&self, payload: &[u8]) -> Result<(), receive::Send> {
        loop {
            match Uring::send::<{ libc::MSG_NOSIGNAL }, Errno>(self.writer.as_fd(), payload) {
                Ok(sent) => {
                    return match receive::Partial::classify(sent.len(), payload.len()) {
                        None => Ok(()),
                        Some(partial) => Err(receive::Send::Partial(partial)),
                    };
                }
                Err(error) if error.is_intr() => {}
                Err(error) => return Err(receive::Send::Syscall(error)),
            }
        }
    }
}

const _: () = assert!(mem::size_of::<Pair>() == 2 * mem::size_of::<fd::OwnedFd>());
