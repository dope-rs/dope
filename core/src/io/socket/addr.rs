use std::{io, net, path};

use crate::io::socket;

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Addr(socket::raw::Addr);

impl Addr {
    pub fn from_std(addr: net::SocketAddr) -> Self {
        Self(socket::raw::Addr::from_std(addr))
    }

    pub fn from_unix_path(path: &path::Path) -> io::Result<Self> {
        socket::raw::Addr::from_unix_path(path).map(Self)
    }

    pub fn into_std(self) -> io::Result<net::SocketAddr> {
        self.0.into_std()
    }

    pub(crate) const STORAGE_CAPACITY: usize = socket::raw::Addr::STORAGE_CAPACITY;

    pub(crate) fn from_raw(addr: socket::raw::Addr) -> Self {
        Self(addr)
    }

    pub(crate) const fn raw(&self) -> &socket::raw::Addr {
        &self.0
    }

    pub(crate) fn parse_msg_name(name: &[u8]) -> Option<net::SocketAddr> {
        if name.len() > Self::STORAGE_CAPACITY {
            return None;
        }
        socket::raw::Addr::parse_msg_name(name)
    }
}
