use std::net;

use crate::runtime::routing;

pub(in crate::client) struct Peer<A, R> {
    pub(in crate::client) addr: net::SocketAddr,
    pub(in crate::client) auth: A,
    pub(in crate::client) route: R,
}

pub(in crate::client) enum Topology<A, R: routing::PeerRoute> {
    Flat(Box<[Peer<A, R>]>),
    Dense {
        paths: Box<[net::SocketAddr]>,
        auth: A,
        shards: routing::Count,
        capacity: usize,
    },
}

impl<A, R: routing::PeerRoute> Topology<A, R> {
    pub(in crate::client) fn dense(
        paths: Vec<net::SocketAddr>,
        auth: A,
        shards: routing::Count,
    ) -> Option<Self> {
        let capacity = shards.dense_len(paths.len())?;
        let last = routing::Shard::from_index((shards.get() - 1) as u8);
        R::from_shard(last)?;
        Some(Self::Dense {
            paths: paths.into_boxed_slice(),
            auth,
            shards,
            capacity,
        })
    }

    pub(in crate::client) fn len(&self) -> usize {
        match self {
            Self::Flat(peers) => peers.len(),
            Self::Dense { capacity, .. } => *capacity,
        }
    }

    pub(in crate::client) fn peer(&self, index: usize) -> Option<(&net::SocketAddr, &A, R)> {
        match self {
            Self::Flat(peers) => peers
                .get(index)
                .map(|peer| (&peer.addr, &peer.auth, peer.route)),
            Self::Dense {
                paths,
                auth,
                shards,
                capacity: _,
            } => {
                let first = R::from_shard(routing::Shard::from_index(0))?;
                if first.shard().is_none() {
                    return Some((paths.get(index)?, auth, first));
                }
                let (shard, path) = shards.resolve(paths.len(), index)?;
                Some((paths.get(path)?, auth, R::from_shard(shard)?))
            }
        }
    }
}
