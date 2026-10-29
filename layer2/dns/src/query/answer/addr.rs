use std::net;

use dope::manifold::service;
use o3::depot::array::{self, copy};

#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct Addresses<const N: usize> {
    values: copy::Vec<net::IpAddr, N>,
}

pub(crate) enum Insert {
    Added,
    Present,
    Full,
}

impl<const N: usize> Addresses<N> {
    pub(crate) const VALID: () = {
        assert!(N != 0, "DNS address capacity must be nonzero");
        assert!(
            N <= service::MAX_ENDPOINTS,
            "DNS address capacity exceeds the service endpoint ceiling"
        );
    };

    pub(crate) fn new() -> Self {
        let () = Self::VALID;
        Self {
            values: copy::Vec::new(),
        }
    }

    pub(crate) fn singleton(value: net::IpAddr) -> Self {
        let () = Self::VALID;
        Self {
            values: copy::Vec::from_one(value),
        }
    }

    pub(crate) fn insert(&mut self, value: net::IpAddr) -> Insert {
        if self.values.as_slice().contains(&value) {
            return Insert::Present;
        }
        let Some(vacant) = self.values.vacancy() else {
            return Insert::Full;
        };
        vacant.insert(value);
        Insert::Added
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn bind(
        self,
        port: u16,
    ) -> array::Vec<service::Endpoint<net::SocketAddr, net::SocketAddr>, N> {
        let mut peers = array::Vec::new();
        peers.map_from(&self.values, |_, address| {
            let addr = net::SocketAddr::new(*address, port);
            service::Endpoint::new(addr, addr)
        });
        peers
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn as_slice(&self) -> &[net::IpAddr] {
        self.values.as_slice()
    }
}
