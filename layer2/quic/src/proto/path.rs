use std::{net, num::NonZero};

use crate::security::relay::Ticket;

/// One physical destination and its exact opaque return route.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Peer {
    address: net::SocketAddr,
    ticket: Option<Ticket>,
}

impl Peer {
    pub const fn direct(address: net::SocketAddr) -> Self {
        Self {
            address,
            ticket: None,
        }
    }

    pub const fn relayed(address: net::SocketAddr, ticket: Ticket) -> Self {
        Self {
            address,
            ticket: Some(ticket),
        }
    }

    pub const fn addr(self) -> net::SocketAddr {
        self.address
    }

    pub const fn ticket(&self) -> Option<&Ticket> {
        self.ticket.as_ref()
    }

    pub(crate) const fn is_relayed(self) -> bool {
        self.ticket.is_some()
    }

    pub(crate) const fn overhead(self) -> usize {
        if self.is_relayed() { Ticket::LEN } else { 0 }
    }

    /// Maximum ordinary UDP payload for this physical IP family.
    pub(crate) const fn max_udp_payload(self) -> NonZero<u16> {
        dope::core::io::datagram::payload::maximum(self.address)
    }

    pub(crate) fn max_payload(self) -> NonZero<u16> {
        let Some(bytes) = NonZero::new(self.max_udp_payload().get() - self.overhead() as u16)
        else {
            unreachable!("payload")
        };
        bytes
    }
}

impl From<net::SocketAddr> for Peer {
    fn from(address: net::SocketAddr) -> Self {
        Self::direct(address)
    }
}
