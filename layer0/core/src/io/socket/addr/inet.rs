use std::{
    cell, marker, net,
    os::{fd, fd::AsFd as _},
};

use dope_sys::{
    Errno,
    io::socket::{BACKLOG, Kind, Socket as _, addr},
};

use crate::{
    Backend,
    io::socket::{Addr, BindError, Prepared, StreamSpec, addr::prepared},
};

#[derive(Clone, Copy)]
pub struct Inet {
    native: addr::Internet,
}

/// An affine TCP listener retaining its established role and local address.
#[must_use]
pub struct Listener {
    fd: fd::OwnedFd,
    local_addr: net::SocketAddr,
    thread: marker::PhantomData<cell::Cell<()>>,
}

impl Listener {
    /// Creates a listening TCP socket before it is transferred to a driver.
    pub fn bind(addr: net::SocketAddr) -> Result<Self, BindError> {
        let address = Inet::from_std(addr);
        let domain = address.stream_spec().domain().raw();
        let fd = Backend::socket(domain, Kind::Stream.raw()).map_err(BindError::socket)?;
        let borrowed = fd.as_fd();
        address
            .view()
            .native()
            .try_bind::<Backend, Errno>(borrowed)
            .map_err(BindError::Bind)?;
        Backend::listen::<Errno>(borrowed, BACKLOG).map_err(BindError::Listen)?;
        let local = Addr::local(borrowed)
            .map_err(BindError::LocalAddress)?
            .into_std()
            .map_err(BindError::Address)?;
        Ok(Self {
            fd,
            local_addr: local,
            thread: marker::PhantomData,
        })
    }

    pub const fn local_addr(&self) -> net::SocketAddr {
        self.local_addr
    }

    pub(in crate::io::socket) fn into_parts(self) -> (fd::OwnedFd, net::SocketAddr) {
        (self.fd, self.local_addr)
    }
}

impl Inet {
    pub fn from_std(address: net::SocketAddr) -> Self {
        Self {
            native: addr::Internet::from_std::<Backend>(address),
        }
    }
}

impl From<&net::SocketAddr> for Inet {
    fn from(address: &net::SocketAddr) -> Self {
        Self::from_std(*address)
    }
}

impl prepared::Sealed for Inet {}

impl Prepared for Inet {
    fn view(&self) -> prepared::View<'_> {
        prepared::View(self.native.as_addr())
    }

    fn stream_spec(&self) -> StreamSpec {
        StreamSpec(self.native.domain())
    }
}

const _: () = assert!(size_of::<Inet>() <= 32);

impl Inet {
    /// Connects the caller's existing socket to this retained address.
    pub fn try_connect(&self, fd: fd::BorrowedFd<'_>) -> Result<(), crate::Errno> {
        self.view().native().try_connect::<Backend, Errno>(fd)
    }
}
