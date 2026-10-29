use std::{net, num};

use dope_core::driver::route::{Identity, table::Capacity};
use o3::depot::{
    self,
    map::{self, flat},
};

use crate::network::listener::runtime::accept::{self, peers};

struct Peer {
    ip: net::IpAddr,
    connections: num::NonZero<u32>,
}

pub(super) struct Table {
    inner: flat::Table<Peer>,
}

impl Table {
    pub(super) fn try_new(capacity: Capacity) -> Result<Self, accept::Prepare> {
        let plan = map::Plan::new(capacity.count().get()).map_err(accept::Prepare::Capacity)?;
        Ok(Self {
            inner: flat::Table::try_new(plan).map_err(accept::Prepare::Allocation)?,
        })
    }

    /// Releases the peer inserted by the exact affine admission.
    pub(super) fn release<ID: Identity>(
        &mut self,
        admit: peers::Admission<'_, ID>,
        ip: net::IpAddr,
        hash: u64,
    ) {
        // SAFETY: Counts transfers its affine admission for this exact table and IP.
        // Its immutable HashState reproduces the insertion hash until this release.
        let source = unsafe {
            depot::raw::Proof::new((&mut self.inner, admit, hash, |peer: &Peer| peer.ip == ip))
        };
        let (mut entry, _) = flat::Table::member(source);
        match num::NonZero::new(entry.get().connections.get() - 1) {
            Some(count) => entry.get_mut().connections = count,
            None => entry.discard(),
        }
    }

    pub(super) fn acquire(
        &mut self,
        ip: net::IpAddr,
        limit: num::NonZero<u32>,
        hash: u64,
    ) -> Result<(), peers::Full> {
        use flat::Entry;

        let Some(entry) = self.inner.entry(hash, |peer| peer.ip == ip) else {
            return Err(peers::Full);
        };
        match entry {
            Entry::Occupied(mut entry) => {
                let peer = entry.get_mut();
                if peer.connections >= limit {
                    return Err(peers::Full);
                }
                peer.connections = peer.connections.saturating_add(1);
                Ok(())
            }
            Entry::Vacant(entry) => {
                entry.insert(Peer {
                    ip,
                    connections: num::NonZero::<u32>::MIN,
                });
                Ok(())
            }
        }
    }
}
