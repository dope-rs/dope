use std::{net, os::fd::AsFd as _};

use crate::{
    Errno,
    io::socket::{self, Domain, Kind, addr, gso},
};

/// A concrete source IP and scope with the endpoint's requested port.
#[derive(Clone, Copy, Debug)]
pub struct Address(net::SocketAddr);

impl Address {
    pub const fn get(self) -> net::SocketAddr {
        self.0
    }
}

/// The exact cold source-selection failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Family {
        bind: net::SocketAddr,
        peer: net::SocketAddr,
    },
    Open(socket::Create),
    Bind(Errno),
    Connect(Errno),
    Local(addr::Local<Errno>),
    Address(addr::Error),
    Unspecified(net::SocketAddr),
}

/// Selects the native ancillary encoding for a fixed datagram source.
pub trait Source: socket::Prepare + addr::Source + gso::Source + Sized {
    type Control: Copy + AsRef<[u8]>;
    /// Owns the selected source and exactly the optional segmentation capability supplied.
    type Combined;

    fn control(source: Address) -> Self::Control;
    fn compose(source: Self::Control, gso: Option<<Self as gso::Source>::Mode>) -> Self::Combined;
    fn release(control: Self::Combined) -> (Self::Control, Option<<Self as gso::Source>::Mode>);
    fn bytes(control: &mut Self::Combined) -> &[u8];

    /// Preserves an explicit source; resolves wildcard binds once on a separate socket.
    fn source(
        bind: net::SocketAddr,
        peer: net::SocketAddr,
    ) -> Result<(Address, Self::Control), Error> {
        if bind.is_ipv4() != peer.is_ipv4() {
            return Err(Error::Family { bind, peer });
        }
        let local = if bind.ip().to_canonical().is_unspecified() {
            let file = Self::open(Domain::for_addr(&peer), Kind::Dgram).map_err(Error::Open)?;
            let mut local = bind;
            local.set_port(0);
            addr::Internet::from_std::<Self>(local)
                .as_addr()
                .try_bind::<Self, Errno>(file.as_fd())
                .map_err(Error::Bind)?;
            addr::Internet::from_std::<Self>(peer)
                .as_addr()
                .try_connect::<Self, Errno>(file.as_fd())
                .map_err(Error::Connect)?;
            let mut storage = addr::Storage::empty();
            addr::Addr::local::<Self, Errno>(file.as_fd(), &mut storage).map_err(Error::Local)?;
            let mut local = storage.to_std().map_err(Error::Address)?;
            if local.ip().to_canonical().is_unspecified() {
                return Err(Error::Unspecified(local));
            }
            local.set_port(bind.port());
            Address(local)
        } else {
            Address(bind)
        };
        Ok((local, Self::control(local)))
    }
}
