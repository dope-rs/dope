use std::{net, net::SocketAddr};

use dope_core::io::socket::{self, Listener};

pub struct Reservation {
    listener: Listener,
    addr: net::SocketAddr,
}

impl Reservation {
    pub fn bind() -> Result<Self, socket::BindError> {
        let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let addr = listener.local_addr();
        Ok(Self { listener, addr })
    }

    pub const fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    pub fn into_tcp(self) -> Listener {
        self.listener
    }
}
