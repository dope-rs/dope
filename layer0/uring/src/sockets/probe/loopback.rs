use std::{mem, os::fd};

use dope_sys::{
    Errno,
    io::socket::{self, Socket as _, addr},
    probe::Creation,
};
use fd::AsFd as _;

use crate::{Uring, sockets::probe};

pub(crate) struct Loopback {
    listener: fd::OwnedFd,
    storage: addr::Storage<addr::Socklen>,
}

impl fd::AsFd for Loopback {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.listener.as_fd()
    }
}

impl Loopback {
    pub(crate) fn new() -> Result<Self, Creation> {
        let listener = probe::bind(socket::Kind::Stream.raw())?;
        Uring::listen::<Errno>(listener.as_fd(), 1)?;
        let mut buffer = addr::Storage::<addr::Length>::empty();
        let storage = addr::Storage::from_addr(addr::Addr::local::<Uring, Errno>(
            listener.as_fd(),
            &mut buffer,
        )?);
        Ok(Self { listener, storage })
    }

    pub(crate) fn connect(&self) -> Result<fd::OwnedFd, Errno> {
        let client = Uring::socket(socket::Domain::Inet4.raw(), socket::Kind::Stream.raw())?;
        self.storage
            .as_addr()
            .try_connect::<Uring, Errno>(client.as_fd())?;
        Ok(client)
    }
}

const _: () = assert!(
    mem::size_of::<Loopback>() == mem::size_of::<(fd::OwnedFd, addr::Storage<addr::Socklen>)>()
);
