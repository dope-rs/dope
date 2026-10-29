pub(in crate::io::socket) mod inet;
pub(in crate::io::socket) mod prepared;
pub(in crate::io::socket) mod unix;

use std::{net, os::fd};

use dope_sys::{Errno, io::socket::addr};

use crate::{Backend, io::socket};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Addr(addr::Storage<addr::Socklen>);

impl Addr {
    pub fn from_std(address: net::SocketAddr) -> Self {
        let native = addr::Internet::from_std::<Backend>(address);
        Self(addr::Storage::from_addr(native.as_addr()))
    }

    /// Copies an already-prepared socket address.
    pub fn copy_from<A: socket::Prepared>(addr: &A) -> Self {
        Self(addr::Storage::from_addr(addr.view().native()))
    }

    pub fn into_std(self) -> Result<net::SocketAddr, socket::Error> {
        self.0.to_std().map_err(socket::Error::from)
    }

    /// Reads the local address bound to a descriptor.
    pub fn local(fd: fd::BorrowedFd<'_>) -> Result<Self, addr::Local<Errno>> {
        let mut buffer = addr::Storage::<addr::Length>::empty();
        let view = addr::Addr::local::<Backend, Errno>(fd, &mut buffer)?;
        Ok(Self(addr::Storage::from_addr(view)))
    }

    pub(crate) const STORAGE_CAPACITY: usize = addr::CAPACITY;

    pub(crate) const fn storage(&self) -> &addr::Storage<addr::Socklen> {
        &self.0
    }

    pub(crate) fn try_parse(name: &[u8]) -> Result<net::SocketAddr, socket::Error> {
        addr::Internet::try_parse(name).map_err(socket::Error::from)
    }
}
